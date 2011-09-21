package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Before;
import org.junit.Test;

public class TestLoggerSink {

  private LoggerSink sink;

  @Before
  public void setUp() {
    sink = new LoggerSink();
  }

  /**
   * Lack of exception test.
   */
  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Channel channel = new MemoryChannel();
    Context context = new Context();
    Configurables.configure(channel, context);
    Configurables.configure(sink, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = EventBuilder.withBody(("Test " + i).getBytes());

      channel.put(event);
      sink.process();
    }

    sink.stop();
  }

}
