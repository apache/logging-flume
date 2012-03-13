/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.ipc.CallFuture;

import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;

/**
 * Avro/Netty implementation of {@link RpcClient}.
 * The connections are intended to be opened before clients are given access so
 * that the object cannot ever be in an inconsistent when exposed to users.
 */
public class NettyAvroRpcClient implements RpcClient {

  private final ReentrantLock stateLock = new ReentrantLock();

  private final static long DEFAULT_CONNECT_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  private final static long DEFAULT_REQUEST_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  /**
   * Guarded by {@code stateLock}
   */
  private ConnState connState;

  private final String hostname;
  private final Integer port;
  private final Integer batchSize;

  private Transceiver transceiver;
  private AvroSourceProtocol.Callback avroClient;

  /**
   * This constructor is intended to be called from {@link AvroClientBuilder}.
   * @param hostname The destination hostname
   * @param port The destination port
   * @param batchSize Maximum number of Events to accept in appendBatch()
   */
  private NettyAvroRpcClient(String hostname, Integer port, Integer batchSize) {

    if (hostname == null) throw new NullPointerException("hostname is null");
    if (port == null) throw new NullPointerException("port is null");
    if (batchSize == null) throw new NullPointerException("batchSize is null");

    this.hostname = hostname;
    this.port = port;
    this.batchSize = batchSize;

    setState(ConnState.INIT);
  }

  /**
   * This method should only be invoked by the Builder
   * @throws FlumeException
   */
  private void connect() throws FlumeException {
    connect(DEFAULT_CONNECT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  /**
   * Internal only, for now
   * @param timeout
   * @param tu
   * @throws FlumeException
   */
  private void connect(long timeout, TimeUnit tu) throws FlumeException {
    try {
      transceiver = new NettyTransceiver(new InetSocketAddress(hostname, port),
          tu.toMillis(timeout));
      avroClient =
          SpecificRequestor.getClient(AvroSourceProtocol.Callback.class,
          transceiver);

    } catch (IOException ex) {
      throw new FlumeException("RPC connection error. Exception follows.", ex);
    }

    setState(ConnState.READY);
  }

  @Override
  public void close() throws FlumeException {
    try {
      transceiver.close();
    } catch (IOException ex) {
      throw new FlumeException("Error closing transceiver. Exception follows.",
          ex);
    } finally {
      setState(ConnState.DEAD);
    }

  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void append(Event event) throws EventDeliveryException {
    try {
      append(event, DEFAULT_REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (EventDeliveryException e) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      throw e;
    }
  }

  private void append(Event event, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    CallFuture<Status> callFuture = new CallFuture<Status>();

    try {
      AvroFlumeEvent avroEvent = new AvroFlumeEvent();
      avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
      avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));
      avroClient.append(avroEvent, callFuture);
    } catch (IOException ex) {
      throw new EventDeliveryException("RPC request IO exception. " +
          "Exception follows.", ex);
    }

    waitForStatusOK(callFuture, timeout, tu);
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    try {
      appendBatch(events, DEFAULT_REQUEST_TIMEOUT_MILLIS,
          TimeUnit.MILLISECONDS);
    } catch (EventDeliveryException e) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      throw e;
    }
  }

  private void appendBatch(List<Event> events, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    Iterator<Event> iter = events.iterator();
    List<AvroFlumeEvent> avroEvents = new LinkedList<AvroFlumeEvent>();

    // send multiple batches... bail if there is a problem at any time
    while (iter.hasNext()) {
      avroEvents.clear();

      for (int i = 0; i < batchSize && iter.hasNext(); i++) {
        Event event = iter.next();
        AvroFlumeEvent avroEvent = new AvroFlumeEvent();
        avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
        avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));
        avroEvents.add(avroEvent);
      }

      CallFuture<Status> callFuture = new CallFuture<Status>();
      try {
        avroClient.appendBatch(avroEvents, callFuture);
      } catch (IOException ex) {
        throw new EventDeliveryException("RPC request IO exception. " +
            "Exception follows.", ex);
      }

      waitForStatusOK(callFuture, timeout, tu);
    }
  }

  /**
   * Helper method that waits for a Status future to come back and validates
   * that it returns Status == OK.
   * @param callFuture Future to wait on
   * @param timeout Time to wait before failing
   * @param tu Time Unit of {@code timeout}
   * @throws EventDeliveryException If there is a timeout or if Status != OK
   */
  private static void waitForStatusOK(CallFuture<Status> callFuture,
      long timeout, TimeUnit tu) throws EventDeliveryException {
    try {
      Status status = callFuture.get(timeout, tu);
      if (status != Status.OK) {
        throw new EventDeliveryException("Status (" + status + ") is not OK");
      }
    } catch (CancellationException ex) {
      throw new EventDeliveryException("RPC future was cancelled." +
          " Exception follows.", ex);
    } catch (ExecutionException ex) {
      throw new EventDeliveryException("Exception thrown from remote handler." +
          " Exception follows.", ex);
    } catch (TimeoutException ex) {
      throw new EventDeliveryException("RPC request timed out." +
          " Exception follows.", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new EventDeliveryException("RPC request interrupted." +
          " Exception follows.", ex);
    }
  }

  /**
   * This method should always be used to change {@code connState} so we ensure
   * that invalid state transitions do not occur and that the {@code isIdle}
   * {@link Condition} variable gets signaled reliably.
   * Throws {@code IllegalStateException} when called to transition from CLOSED
   * to another state.
   * @param state
   */
  private void setState(ConnState newState) {
    stateLock.lock();
    try {
      if (connState == ConnState.DEAD && connState != newState) {
        throw new IllegalStateException("Cannot transition from CLOSED state.");
      }
      connState = newState;
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * If the connection state != READY, throws {@link EventDeliveryException}.
   */
  private void assertReady() throws EventDeliveryException {
    stateLock.lock();
    try {
      ConnState curState = connState;
      if (curState != ConnState.READY) {
        throw new EventDeliveryException("RPC failed, client in an invalid " +
            "state: " + curState);
      }
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
      Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
        new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  @Override
  public boolean isActive() {
    stateLock.lock();
    try {
      return (connState == ConnState.READY);
    } finally {
      stateLock.unlock();
    }
  }

  private static enum ConnState {
    INIT, READY, DEAD
  }

  /**
   * <p>Builder class used to construct {@link NettyAvroRpcClient} objects.</p>
   *
   * <p><strong>Note:</strong> It is recommended for applications to construct
   * {@link RpcClient} instances using the {@link RpcClientFactory} class.</p>
   */
  protected static class Builder {

    protected static final int DEFAULT_BATCH_SIZE = 100;

    private String hostname;
    private Integer port;
    private Integer batchSize;

    public Builder() {
      batchSize = DEFAULT_BATCH_SIZE;
    }

    /**
     * Hostname to connect to (required)
     *
     * @param hostname
     * @return {@code this}
     */
    public Builder hostname(String hostname) {
      if (hostname == null) {
        throw new NullPointerException("hostname is null");
      }

      this.hostname = hostname;
      return this;
    }

    /**
     * Port to connect to (required)
     *
     * @param port
     * @return {@code this}
     */
    public Builder port(Integer port) {
      if (port == null) {
        throw new NullPointerException("port is null");
      }

      this.port = port;
      return this;
    }

    /**
     * Maximum number of {@linkplain Event events} that can be processed in a
     * batch operation. (optional)<br>
     * Default: 100
     *
     * @param batchSize
     * @return {@code this}
     */
    public Builder batchSize(Integer batchSize) {
      if (batchSize == null) {
        throw new NullPointerException("batch size is null");
      }

      this.batchSize = batchSize;
      return this;
    }

    /**
     * Construct the object
     * @return Active RPC client
     * @throws FlumeException
     */
    public NettyAvroRpcClient build() throws FlumeException {
      // validate the required fields
      if (hostname == null) throw new NullPointerException("hostname is null");
      if (port == null) throw new NullPointerException("port is null");
      if (batchSize == null) {
        throw new NullPointerException("batch size is null");
      }

      NettyAvroRpcClient client =
          new NettyAvroRpcClient(hostname, port, batchSize);
      client.connect();

      return client;
    }
  }

}
