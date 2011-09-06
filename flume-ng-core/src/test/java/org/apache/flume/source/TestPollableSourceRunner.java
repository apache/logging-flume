package org.apache.flume.source;

import java.util.concurrent.CountDownLatch;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Before;
import org.junit.Test;

public class TestPollableSourceRunner {

  private PollableSourceRunner sourceRunner;

  @Before
  public void setUp() {
    sourceRunner = new PollableSourceRunner();
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    final Channel channel = new MemoryChannel();
    final CountDownLatch latch = new CountDownLatch(50);

    PollableSource source = new PollableSource() {

      @Override
      public Channel getChannel() {
        // Doesn't matter.
        return null;
      }

      @Override
      public void setChannel(Channel channel) {
        // Doesn't matter.
      }

      @Override
      public void process() throws InterruptedException, EventDeliveryException {
        Event event = EventBuilder.withBody(String.valueOf(
            "Event " + latch.getCount()).getBytes());

        latch.countDown();

        if (latch.getCount() % 20 == 0) {
          throw new EventDeliveryException("I don't like event:" + event);
        }

        channel.put(event);
      }

      @Override
      public void start() {
        // Unused.
      }

      @Override
      public void stop() {
        // Unused.
      }

      @Override
      public LifecycleState getLifecycleState() {
        // Unused.
        return null;
      }

    };

    sourceRunner.setSource(source);
    sourceRunner.start();

    latch.await();

    sourceRunner.stop();
  }

}
