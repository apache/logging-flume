package org.apache.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSequenceGeneratorSource {

  private PollableSource source;

  @Before
  public void setUp() {
    source = new SequenceGeneratorSource();
  }

  @Test
  public void testProcess() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Channel channel = new MemoryChannel();
    Context context = new Context();

    context.put("logicalNode.name", "test");

    Configurables.configure(source, context);

    source.setChannel(channel);

    for (long i = 0; i < 100; i++) {
      source.process();
      Event event = channel.take();

      Assert.assertArrayEquals(String.valueOf(i).getBytes(),
          new String(event.getBody()).getBytes());
    }
  }

  @Test
  public void testLifecycle() throws InterruptedException,
      EventDeliveryException {

    Channel channel = new MemoryChannel();
    Context context = new Context();

    context.put("logicalNode.name", "test");

    Configurables.configure(source, context);

    source.setChannel(channel);
    source.start();

    for (long i = 0; i < 100; i++) {
      source.process();
      Event event = channel.take();

      Assert.assertArrayEquals(String.valueOf(i).getBytes(),
          new String(event.getBody()).getBytes());
    }

    source.stop();
  }

}
