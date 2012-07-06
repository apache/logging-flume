/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

import java.util.List;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.source.AvroSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A {@link Sink} implementation that can send events to an RPC server (such as
 * Flume's {@link AvroSource}).
 * </p>
 * <p>
 * This sink forms one half of Flume's tiered collection support. Events sent to
 * this sink are transported over the network to the hostname / port pair using
 * the RPC implementation encapsulated in {@link RpcClient}.
 * The destination is an instance of Flume's {@link AvroSource}, which
 * allows Flume agents to forward to other Flume agents, forming a tiered
 * collection infrastructure. Of course, nothing prevents one from using this
 * sink to speak to other custom built infrastructure that implements the same
 * RPC protocol.
 * </p>
 * <p>
 * Events are taken from the configured {@link Channel} in batches of the
 * configured <tt>batch-size</tt>. The batch size has no theoretical limits
 * although all events in the batch <b>must</b> fit in memory. Generally, larger
 * batches are far more efficient, but introduce a slight delay (measured in
 * millis) in delivery. The batch behavior is such that underruns (i.e. batches
 * smaller than the configured batch size) are possible. This is a compromise
 * made to maintain low latency of event delivery. If the channel returns a null
 * event, meaning it is empty, the batch is immediately sent, regardless of
 * size. Batch underruns are tracked in the metrics. Empty batches do not incur
 * an RPC roundtrip.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit (data type)</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>hostname</tt></td>
 * <td>The hostname to which events should be sent.</td>
 * <td>Hostname or IP (String)</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which events should be sent on <tt>hostname</tt>.</td>
 * <td>TCP port (int)</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>batch-size</tt></td>
 * <td>The maximum number of events to send per RPC.</td>
 * <td>events (int)</td>
 * <td>100</td>
 * </tr>
 * <tr>
 * <td><tt>connect-timeout</tt></td>
 * <td>Maximum time to wait for the first Avro handshake and RPC request</td>
 * <td>milliseconds (long)</td>
 * <td>20000</td>
 * </tr>
 * <tr>
 * <td><tt>request-timeout</tt></td>
 * <td>Maximum time to wait RPC requests after the first</td>
 * <td>milliseconds (long)</td>
 * <td>20000</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(AvroSink.class);

  private String hostname;
  private Integer port;

  private RpcClient client;
  private Properties clientProps;
  private SinkCounter sinkCounter;

  @Override
  public void configure(Context context) {
    clientProps = new Properties();

    hostname = context.getString("hostname");
    port = context.getInteger("port");

    Preconditions.checkState(hostname != null, "No hostname specified");
    Preconditions.checkState(port != null, "No port specified");

    clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +
        "h1", hostname + ":" + port);

    Integer batchSize = context.getInteger("batch-size");
    if (batchSize != null) {
      clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE,
          String.valueOf(batchSize));
    }

    Long connectTimeout = context.getLong("connect-timeout");
    if (connectTimeout != null) {
      clientProps.setProperty(
          RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT,
          String.valueOf(connectTimeout));
    }

    Long requestTimeout = context.getLong("request-timeout");
    if (requestTimeout != null) {
      clientProps.setProperty(
          RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT,
          String.valueOf(requestTimeout));
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  /**
   * If this function is called successively without calling
   * {@see #destroyConnection()}, only the first call has any effect.
   * @throws FlumeException if an RPC client connection could not be opened
   */
  private void createConnection() throws FlumeException {

    if (client == null) {
      logger.info("Avro sink {}: Building RpcClient with hostname: {}, " +
          "port: {}",
          new Object[] { getName(), hostname, port });
      try {
        client = RpcClientFactory.getInstance(clientProps);
        sinkCounter.incrementConnectionCreatedCount();
      } catch (Exception ex) {
        sinkCounter.incrementConnectionFailedCount();
        if (ex instanceof FlumeException) {
          throw (FlumeException) ex;
        } else {
          throw new FlumeException(ex);
        }
      }
       logger.debug("Avro sink {}: Created RpcClient: {}", getName(), client);
    }

  }

  private void destroyConnection() {
    if (client != null) {
      logger.debug("Avro sink {} closing avro client: {}", getName(), client);
      try {
        client.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (FlumeException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Avro sink " + getName() + ": Attempt to close avro " +
            "client failed. Exception follows.", e);
      }
    }

    client = null;
  }

  /**
   * Ensure the connection exists and is active.
   * If the connection is not active, destroy it and recreate it.
   *
   * @throws FlumeException If there are errors closing or opening the RPC
   * connection.
   */
  private void verifyConnection() throws FlumeException {
    if (client == null) {
      createConnection();
    } else if (!client.isActive()) {
      destroyConnection();
      createConnection();
    }
  }

  /**
   * The start() of AvroSink is more of an optimization that allows connection
   * to be created before the process() loop is started. In case it so happens
   * that the start failed, the process() loop will itself attempt to reconnect
   * as necessary. This is the expected behavior since it is possible that the
   * downstream source becomes unavailable in the middle of the process loop
   * and the sink will have to retry the connection again.
   */
  @Override
  public void start() {
    logger.info("Starting {}...", this);
    sinkCounter.start();
    try {
      createConnection();
    } catch (FlumeException e) {
      logger.warn("Unable to create avro client using hostname: " + hostname
          + ", port: " + port, e);

      /* Try to prevent leaking resources. */
      destroyConnection();
    }

    super.start();

    logger.info("Avro sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("Avro sink {} stopping...", getName());

    destroyConnection();
    sinkCounter.stop();
    super.stop();

    logger.info("Avro sink {} stopped. Metrics: {}", getName(), sinkCounter);
  }

  @Override
  public String toString() {
    return "AvroSink " + getName() + " { host: " + hostname + ", port: " +
        port + " }";
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      verifyConnection();

      List<Event> batch = Lists.newLinkedList();

      for (int i = 0; i < client.getBatchSize(); i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }

        batch.add(event);
      }

      int size = batch.size();
      int batchSize = client.getBatchSize();

      if (size == 0) {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
      } else {
        if (size < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }
        sinkCounter.addToEventDrainAttemptCount(size);
        client.appendBatch(batch);
      }

      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(size);

    } catch (Throwable t) {
      transaction.rollback();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof ChannelException) {
        logger.error("Avro Sink " + getName() + ": Unable to get event from" +
            " channel " + channel.getName() + ". Exception follows.", t);
        status = Status.BACKOFF;
      } else {
        destroyConnection();
        throw new EventDeliveryException("Failed to send events", t);
      }
    } finally {
      transaction.close();
    }

    return status;
  }

}
