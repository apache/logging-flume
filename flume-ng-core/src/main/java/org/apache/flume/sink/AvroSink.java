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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link Sink} implementation that can send events to an Avro server (such as
 * Flume's <tt>AvroSource</tt>).
 * </p>
 * <p>
 * This sink forms one half of Flume's tiered collection support. Events sent to
 * this sink are turned into {@link AvroFlumeEvent}s and sent to the configured
 * hostname / port pair using Avro's {@link NettyTransceiver}. The intent is
 * that the destination is an instance of Flume's <tt>AvroSource</tt> which
 * allows Flume to nodes to forward to other Flume nodes forming a tiered
 * collection infrastructure. Of course, nothing prevents one from using this
 * sink to speak to other custom built infrastructure that implements the same
 * Avro protocol (specifically {@link AvroSourceProtocol}).
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
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>hostname</tt></td>
 * <td>The hostname to which events should be sent.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which events should be sent on <tt>hostname</tt>.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>batch-size</tt></td>
 * <td>The maximum number of events to send per RPC.</td>
 * <td>events / int</td>
 * <td>100</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSink extends AbstractSink implements PollableSink,
    Configurable {

  private static final Logger logger = LoggerFactory.getLogger(AvroSink.class);
  private static final Integer defaultBatchSize = 100;

  private String hostname;
  private Integer port;
  private Integer batchSize;

  private AvroSourceProtocol client;
  private Transceiver transceiver;
  private CounterGroup counterGroup;

  public AvroSink() {
    counterGroup = new CounterGroup();
  }

  @Override
  public void configure(Context context) {
    hostname = context.get("hostname", String.class);
    port = Integer.parseInt(context.get("port", String.class));
    batchSize = Integer.parseInt(context.get("batch-size", String.class));

    if (batchSize == null) {
      batchSize = defaultBatchSize;
    }

    Preconditions.checkState(hostname != null, "No hostname specified");
    Preconditions.checkState(port != null, "No port specified");
  }

  private void createConnection() throws IOException {
    if (transceiver == null) {
      logger.debug("Creating new tranceiver connection to hostname:{} port:{}",
          hostname, port);
      transceiver = new NettyTransceiver(new InetSocketAddress(hostname, port));
    }

    if (client == null) {
      logger.debug("Creating Avro client with tranceiver:{}", transceiver);
      client = SpecificRequestor.getClient(AvroSourceProtocol.class,
          transceiver);
    }
  }

  private void destroyConnection() {
    if (transceiver != null) {
      logger.debug("Destroying tranceiver:{}", transceiver);
      try {
        transceiver.close();
      } catch (IOException e) {
        logger
            .error(
                "Attempt to clean up avro tranceiver after client error failed. Exception follows.",
                e);
      }

      transceiver = null;
    }

    client = null;
  }

  @Override
  public void start() {
    logger.info("Avro sink starting");

    try {
      createConnection();
    } catch (Exception e) {
      logger.error("Unable to create avro client using hostname:" + hostname
          + " port:" + port + ". Exception follows.", e);

      /* Try to prevent leaking resources. */
      destroyConnection();

      /* FIXME: Mark ourselves as failed. */
      return;
    }

    super.start();

    logger.debug("Avro sink started");
  }

  @Override
  public void stop() {
    logger.info("Avro sink stopping");

    destroyConnection();

    super.stop();

    logger.debug("Avro sink stopped. Metrics:{}", counterGroup);
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      createConnection();

      List<AvroFlumeEvent> batch = new LinkedList<AvroFlumeEvent>();

      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();

        if (event == null) {
          counterGroup.incrementAndGet("batch.underflow");
          break;
        }

        AvroFlumeEvent avroEvent = new AvroFlumeEvent();

        avroEvent.body = ByteBuffer.wrap(event.getBody());
        avroEvent.headers = new HashMap<CharSequence, CharSequence>();

        for (Entry<String, String> entry : event.getHeaders().entrySet()) {
          avroEvent.headers.put(entry.getKey(), entry.getValue());
        }

        batch.add(avroEvent);
      }

      if (batch.isEmpty()) {
        counterGroup.incrementAndGet("batch.empty");
        status = Status.BACKOFF;
      } else {
        if (!client.appendBatch(batch).equals(
            org.apache.flume.source.avro.Status.OK)) {
          throw new AvroRemoteException("RPC communication returned FAILED");
        }
      }

      transaction.commit();
      counterGroup.incrementAndGet("batch.success");
    } catch (ChannelException e) {
      transaction.rollback();
      logger.error("Unable to get event from channel. Exception follows.", e);
      status = Status.BACKOFF;
    } catch (AvroRemoteException e) {
      transaction.rollback();
      logger.error("Unable to send event batch. Exception follows.", e);
      status = Status.BACKOFF;
    } catch (Exception e) {
      transaction.rollback();
      logger.error(
          "Unable to communicate with Avro server. Exception follows.", e);
      status = Status.BACKOFF;
      destroyConnection();
    } finally {
      transaction.close();
    }

    return status;
  }

}
