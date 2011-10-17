package org.apache.flume.source;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSource {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSource.class);

  private int selectedPort;
  private AvroSource source;
  private Channel channel;

  @Before
  public void setUp() {
    source = new AvroSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    source.setChannel(channel);
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", selectedPort = 41414 + i);
        context.put("bind", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  @Test
  public void testRequest() throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", selectedPort = 41414 + i);
        context.put("bind", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort)));

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();

    avroEvent.headers = new HashMap<CharSequence, CharSequence>();
    avroEvent.body = ByteBuffer.wrap("Hello avro".getBytes());

    Status status = client.append(avroEvent);

    Assert.assertEquals(Status.OK, status);
 
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();


    logger.debug("Round trip event:{}", event);

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

}
