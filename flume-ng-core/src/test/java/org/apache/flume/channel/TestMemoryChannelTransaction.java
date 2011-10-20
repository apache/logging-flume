package org.apache.flume.channel;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.Transaction.TransactionState;
import org.apache.flume.channel.MemoryChannel.MemTransaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryChannelTransaction {

  private Channel channel;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
  }

  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {

    Event event, event2;
    Context context = new Context();
    int putCounter = 0;

    context.put("keep-alive", "1");
    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    for (putCounter = 0; putCounter < 10; putCounter++) {
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
      // System.out.println(event2.toString());
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.commit();
    transaction.close();
  }

  @Test
  public void testRollBack() throws InterruptedException,
      EventDeliveryException {

    Event event, event2;
    Context context = new Context();
    int putCounter = 0;

    context.put("keep-alive", "1");
    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    // add events and rollback txn
    transaction.begin();
    for (putCounter = 0; putCounter < 10; putCounter++) {
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
    }
    transaction.rollback();
    transaction.close();

    // verify that no events are stored due to rollback
    transaction = channel.getTransaction();
    transaction.begin();
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);
    transaction.commit();
    transaction.close();

    // add events and commit
    transaction = channel.getTransaction();
    transaction.begin();
    for (putCounter = 0; putCounter < 10; putCounter++) {
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    // verify events are there, then rollback the take
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.rollback();
    transaction.close();

    // verify that the events were left in there due to rollback
    transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.rollback();
    transaction.close();
  }

  @Test
  public void testReEntTxn() throws InterruptedException,
      EventDeliveryException {

    Event event, event2;
    Context context = new Context();
    int putCounter = 0;

    context.put("keep-alive", "1");
    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin(); // first begin
    for (putCounter = 0; putCounter < 10; putCounter++) {
      transaction.begin(); // inner begin
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
      transaction.commit(); // inner commit
      Assert.assertEquals(((MemTransaction) transaction).getState(),
          TransactionState.Started);
    }
    transaction.commit();
    Assert.assertEquals(((MemTransaction) transaction).getState(),
        TransactionState.Committed);
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
      // System.out.println(event2.toString());
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.commit();
    transaction.close();
  }

  @Test
  public void testReEntTxnRollBack() throws InterruptedException,
      EventDeliveryException {
    Event event, event2;
    Context context = new Context();
    int putCounter = 0;

    context.put("keep-alive", "1");
    Configurables.configure(channel, context);

    Transaction transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    // add events and rollback txn
    transaction.begin();
    for (putCounter = 0; putCounter < 10; putCounter++) {
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
    }
    transaction.rollback();
    transaction.close();

    // verify that no events are stored due to rollback
    transaction = channel.getTransaction();
    transaction.begin();
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);
    transaction.commit();
    transaction.close();

    // add events and commit
    transaction = channel.getTransaction();
    transaction.begin();
    for (putCounter = 0; putCounter < 10; putCounter++) {
      event = EventBuilder.withBody(("test event" + putCounter).getBytes());
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    // verify events are there, then rollback the take
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      transaction.begin(); // inner begin
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
      transaction.commit(); // inner commit
      Assert.assertEquals(((MemTransaction) transaction).getState(),
          TransactionState.Started);
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.rollback();
    Assert.assertEquals(((MemTransaction) transaction).getState(),
        TransactionState.RolledBack);
    transaction.close();

    // verify that the events were left in there due to rollback
    transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = channel.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
    }
    event2 = channel.take();
    Assert.assertNull("extra event found", event2);

    transaction.rollback();
    transaction.close();
  }

}
