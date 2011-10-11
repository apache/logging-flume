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
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
    port = context.get("port", Integer.class);
    batchSize = context.get("batch-size", Integer.class);

    if (batchSize == null) {
      batchSize = defaultBatchSize;
    }

    Preconditions.checkState(hostname != null, "No hostname specified");
    Preconditions.checkState(port != null, "No port specified");
  }

  @Override
  public void start() {
    logger.info("Avro sink starting");

    try {
      transceiver = new NettyTransceiver(new InetSocketAddress(hostname, port));
      client = SpecificRequestor.getClient(AvroSourceProtocol.class,
          transceiver);
    } catch (Exception e) {
      logger.error("Unable to create avro client using hostname:" + hostname
          + " port:" + port + ". Exception follows.", e);

      /* Try to prevent leaking resources. */
      if (transceiver != null) {
        try {
          transceiver.close();
        } catch (IOException e1) {
          logger
              .error(
                  "Attempt to clean up avro tranceiver after client error failed. Exception follows.",
                  e1);
        }
      }

      /* FIXME: Mark ourselves as failed. */
      return;
    }

    super.start();

    logger.debug("Avro sink started");
  }

  @Override
  public void stop() {
    logger.info("Avro sink stopping");

    try {
      transceiver.close();
    } catch (IOException e) {
      logger.error(
          "Unable to shut down avro tranceiver - Possible resource leak!", e);
    }

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
    } finally {
      transaction.close();
    }

    return status;
  }

}
