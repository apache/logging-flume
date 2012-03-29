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
import java.util.Properties;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro/Netty implementation of {@link RpcClient}.
 * The connections are intended to be opened before clients are given access so
 * that the object cannot ever be in an inconsistent when exposed to users.
 */
public class NettyAvroRpcClient extends AbstractRpcClient
implements RpcClient {

  private final ReentrantLock stateLock = new ReentrantLock();

  private final static long DEFAULT_CONNECT_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  private final static long DEFAULT_REQUEST_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS);

  /**
   * Guarded by {@code stateLock}
   */
  private ConnState connState;

  private InetSocketAddress address;

  private Transceiver transceiver;
  private AvroSourceProtocol.Callback avroClient;
  private static final Logger logger = LoggerFactory
      .getLogger(NettyAvroRpcClient.class);

  /**
   * This constructor is intended to be called from {@link RpcClientFactory}.
   * @param address The InetSocketAddress to connect to
   * @param batchSize Maximum number of Events to accept in appendBatch()
   */
  protected NettyAvroRpcClient(InetSocketAddress address, Integer batchSize)
      throws FlumeException{
    if (address == null){
      logger.error("InetSocketAddress is null, cannot create client.");
      throw new NullPointerException("InetSocketAddress is null");
    }
    this.address = address;
    if(batchSize == null || batchSize == 0){
      this.batchSize = DEFAULT_BATCH_SIZE;
    }
    else{
    this.batchSize = batchSize;
    }
    connect();
  }

  /**
   * This constructor is intended to be called from {@link RpcClientFactory}.
   * A call to this constructor should be followed by call to configure().
   */
  protected NettyAvroRpcClient(){
  }

  /**
   * This method should only be invoked by the build function
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
      transceiver = new NettyTransceiver(this.address, tu.toMillis(timeout));
      avroClient =
          SpecificRequestor.getClient(AvroSourceProtocol.Callback.class,
          transceiver);
    } catch (IOException ex) {
      logger.error("RPC connection error :" , ex);
      throw new FlumeException("RPC connection error. Exception follows.", ex);
    }

    setState(ConnState.READY);
  }

  @Override
  public void close() throws FlumeException {
    try {
      transceiver.close();
    } catch (IOException ex) {
      logger.error("Error closing transceiver. " , ex);
      throw new FlumeException("Error closing transceiver. Exception follows.",
          ex);
    } finally {
      setState(ConnState.DEAD);
    }

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
      logger.error("RPC request IO exception. " , ex);
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
        logger.error("RPC request IO exception. " , ex);
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
        logger.error("Status (" + status + ") is not OK");
        throw new EventDeliveryException("Status (" + status + ") is not OK");
      }
    } catch (CancellationException ex) {
      logger.error("RPC future was cancelled." , ex);
      throw new EventDeliveryException("RPC future was cancelled." +
          " Exception follows.", ex);
    } catch (ExecutionException ex) {
      logger.error("Exception thrown from remote handler." , ex);
      throw new EventDeliveryException("Exception thrown from remote handler." +
          " Exception follows.", ex);
    } catch (TimeoutException ex) {
      logger.error("RPC request timed out." , ex);
      throw new EventDeliveryException("RPC request timed out." +
          " Exception follows.", ex);
    } catch (InterruptedException ex) {
      logger.error("RPC request interrupted." , ex);
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
        logger.error("Cannot transition from CLOSED state.");
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
        logger.error("RPC failed, client in an invalid state: " + curState);
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
   * <p>
   * Configure the actual client using the properties.
   * <tt>properties</tt> should have at least 2 params:
   * <p><tt>hosts</tt> = <i>alias_for_host</i></p>
   * <p><tt>alias_for_host</tt> = <i>hostname:port</i>. </p>
   * Only the first host is added, rest are discarded.</p>
   * <p>Optionally it can also have a <p>
   * <tt>batch-size</tt> = <i>batchSize</i>
   * @param properties The properties to instantiate the client with.
   * @return
     */
  @Override
  public synchronized void configure(Properties properties)
      throws FlumeException {
    stateLock.lock();
    try{
      if(connState == ConnState.READY || connState == ConnState.DEAD){
        logger.error("This client was already configured, " +
            "cannot reconfigure.");
        throw new FlumeException("This client was already configured, " +
            "cannot reconfigure.");
    }
    } finally {
      stateLock.unlock();
      }
    String strbatchSize = properties.getProperty("batch-size");
    batchSize = DEFAULT_BATCH_SIZE;
    if (strbatchSize != null && !strbatchSize.isEmpty()) {
      try {
        batchSize = Integer.parseInt(strbatchSize);
      } catch (NumberFormatException e) {
        logger.warn("Batchsize is not valid for RpcClient: " + strbatchSize +
            ".Default value assigned.", e);
      }
    }
    String hostNames = properties.getProperty(CONFIG_HOSTS);
    String[] hosts = null;
    if (hostNames != null && !hostNames.isEmpty()) {
      hosts = hostNames.split("\\s+");
    } else {
      logger.error("Hosts list is invalid: "+ hostNames);
      throw new FlumeException("Hosts list is invalid: "+ hostNames);
    }
    String host = properties.getProperty(HOSTS_PREFIX+hosts[0]);
    if (host == null || host.isEmpty()) {
      logger.error("Host not found: " + hosts[0]);
      throw new FlumeException("Host not found: " + hosts[0]);
    }
    String[] hostAndPort = host.split(":");
    if (hostAndPort.length != 2){
      logger.error("Invalid hostname, " + hosts[0]);
      throw new FlumeException("Invalid hostname, " + hosts[0]);
    }
    Integer port = null;
    try {
      port = Integer.parseInt(hostAndPort[1]);
    } catch (NumberFormatException e) {
      logger.error("Invalid Port:" + hostAndPort[1], e);
      throw new FlumeException("Invalid Port:" + hostAndPort[1], e);
    }
    this.address = new InetSocketAddress(hostAndPort[0], port);
    this.connect();
  }

}
