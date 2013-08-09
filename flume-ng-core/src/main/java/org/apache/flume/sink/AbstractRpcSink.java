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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This sink provides the basic RPC functionality for Flume. This sink takes
 * several arguments which are used in RPC.
 * This sink forms one half of Flume's tiered collection support. Events sent to
 * this sink are transported over the network to the hostname / port pair using
 * the RPC implementation encapsulated in {@link RpcClient}.
 * The destination is an instance of Flume's {@link org.apache.flume.source
 * .AvroSource} or {@link org.apache.flume.source.ThriftSource} (based on
 * which implementation of this class is used), which
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
 * <tr>
 * <td><tt>compression-type</tt></td>
 * <td>Select compression type.  Default is "none" and the only compression type available is "deflate"</td>
 * <td>compression type</td>
 * <td>none</td>
 * </tr>
 * <tr>
 * <td><tt>compression-level</tt></td>
 * <td>In the case compression type is "deflate" this value can be between 0-9.  0 being no compression and
 * 1-9 is compression.  The higher the number the better the compression.  6 is the default.</td>
 * <td>compression level</td>
 * <td>6</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 *
 * <strong>Implementation Notes:</strong> Any implementation of this class
 * must override the {@linkplain #initializeRpcClient(Properties)} method.
 * This method will be called whenever this sink needs to create a new
 * connection to the source.
 */
public abstract class AbstractRpcSink extends AbstractSink
  implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger
    (AbstractRpcSink.class);
  private String hostname;
  private Integer port;
  private RpcClient client;
  private Properties clientProps;
  private SinkCounter sinkCounter;
  private int cxnResetInterval;
  private AtomicBoolean resetConnectionFlag;
  private final int DEFAULT_CXN_RESET_INTERVAL = 0;
  private final ScheduledExecutorService cxnResetExecutor = Executors
    .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("Rpc Sink Reset Thread").build());

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

    for (Entry<String, String> entry: context.getParameters().entrySet()) {
      clientProps.setProperty(entry.getKey(), entry.getValue());
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
    cxnResetInterval = context.getInteger("reset-connection-interval",
      DEFAULT_CXN_RESET_INTERVAL);
    if(cxnResetInterval == DEFAULT_CXN_RESET_INTERVAL) {
      logger.info("Connection reset is set to " + String.valueOf
        (DEFAULT_CXN_RESET_INTERVAL) +". Will not reset connection to next " +
        "hop");
    }
  }

  /**
   * Returns a new {@linkplain RpcClient} instance configured using the given
   * {@linkplain Properties} object. This method is called whenever a new
   * connection needs to be created to the next hop.
   * @param props
   * @return
   */
  protected abstract RpcClient initializeRpcClient(Properties props);

  /**
   * If this function is called successively without calling
   * {@see #destroyConnection()}, only the first call has any effect.
   * @throws org.apache.flume.FlumeException if an RPC client connection could not be opened
   */
  private void createConnection() throws FlumeException {

    if (client == null) {
      logger.info("Rpc sink {}: Building RpcClient with hostname: {}, " +
          "port: {}",
          new Object[] { getName(), hostname, port });
      try {
        resetConnectionFlag = new AtomicBoolean(false);
        client = initializeRpcClient(clientProps);
        Preconditions.checkNotNull(client, "Rpc Client could not be " +
          "initialized. " + getName() + " could not be started");
        sinkCounter.incrementConnectionCreatedCount();
        if (cxnResetInterval > 0) {
          cxnResetExecutor.schedule(new Runnable() {
            @Override
            public void run() {
              resetConnectionFlag.set(true);
            }
          }, cxnResetInterval, TimeUnit.SECONDS);
        }
      } catch (Exception ex) {
        sinkCounter.incrementConnectionFailedCount();
        if (ex instanceof FlumeException) {
          throw (FlumeException) ex;
        } else {
          throw new FlumeException(ex);
        }
      }
       logger.debug("Rpc sink {}: Created RpcClient: {}", getName(), client);
    }

  }

  private void resetConnection() {
      try {
        destroyConnection();
        createConnection();
      } catch (Throwable throwable) {
        //Don't rethrow, else this runnable won't get scheduled again.
        logger.error("Error while trying to expire connection",
          throwable);
      }
  }

  private void destroyConnection() {
    if (client != null) {
      logger.debug("Rpc sink {} closing Rpc client: {}", getName(), client);
      try {
        client.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (FlumeException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Rpc sink " + getName() + ": Attempt to close Rpc " +
            "client failed. Exception follows.", e);
      }
    }

    client = null;
  }

  /**
   * Ensure the connection exists and is active.
   * If the connection is not active, destroy it and recreate it.
   *
   * @throws org.apache.flume.FlumeException If there are errors closing or opening the RPC
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
   * The start() of RpcSink is more of an optimization that allows connection
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
      logger.warn("Unable to create Rpc client using hostname: " + hostname
          + ", port: " + port, e);

      /* Try to prevent leaking resources. */
      destroyConnection();
    }

    super.start();

    logger.info("Rpc sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("Rpc sink {} stopping...", getName());

    destroyConnection();
    cxnResetExecutor.shutdown();
    try {
      if (cxnResetExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cxnResetExecutor.shutdownNow();
      }
    } catch (Exception ex) {
      logger.error("Interrupted while waiting for connection reset executor " +
        "to shut down");
    }
    sinkCounter.stop();
    super.stop();

    logger.info("Rpc sink {} stopped. Metrics: {}", getName(), sinkCounter);
  }

  @Override
  public String toString() {
    return "RpcSink " + getName() + " { host: " + hostname + ", port: " +
        port + " }";
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    if(resetConnectionFlag.get()) {
      resetConnection();
      // if the time to reset is long and the timeout is short
      // this may cancel the next reset request
      // this should however not be an issue
      resetConnectionFlag.set(false);
    }

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
        logger.error("Rpc Sink " + getName() + ": Unable to get event from" +
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

  @VisibleForTesting
  RpcClient getUnderlyingClient() {
    return client;
  }
}
