package org.apache.flume.sink;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSink.class);
  private static final String hostname = "localhost";
  private static final Integer port = 41414;

  private AvroSink sink;
  private Channel channel;

  @Before
  public void setUp() {
    sink = new AvroSink();
    channel = new MemoryChannel();

    Context context = new Context();

    context.put("hostname", "localhost");
    context.put("port", 41414);
    context.put("batch-size", 2);

    sink.setChannel(channel);

    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    Server server = createServer();

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testProcess() throws InterruptedException, EventDeliveryException {
    Event event = EventBuilder.withBody("test event 1".getBytes(),
        new HashMap<String, String>());
    Server server = createServer();

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }

    for (int i = 0; i < 5; i++) {
      PollableSink.Status status = sink.process();
      Assert.assertEquals(PollableSink.Status.READY, status);
    }

    Assert.assertEquals(PollableSink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  private Server createServer() {
    Server server = new NettyServer(new SpecificResponder(
        AvroSourceProtocol.class, new MockAvroServer()), new InetSocketAddress(
        hostname, port));

    return server;
  }

  private static class MockAvroServer implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.debug("Received event:{}", event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {

      logger.debug("Received event batch:{}", events);

      return Status.OK;
    }

  }

}
