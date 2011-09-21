package org.apache.flume.channel;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryChannel {

  private Channel channel;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
  }

  @Test
  public void testPutTake() throws InterruptedException, EventDeliveryException {
    Event event = EventBuilder.withBody("test event".getBytes());
    Context context = new Context();

    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    channel.put(event);
    transaction.commit();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    Event event2 = channel.take();
    Assert.assertEquals(event, event2);
    transaction.commit();
  }

}
