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
import org.junit.Ignore;
import org.junit.Test;

public class TestMemoryChannelTransaction {

  private Channel channel;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
  }

  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {

    Event event;
    Event event2;
    Context context = new Context();
    int putCounter = 0;

    context.put("keep-alive", "1");
    context.put("capacity", "100");
    context.put("transactionCapacity", "50");
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

    Event event;
    Event event2;
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

  @Ignore("BasicChannelSemantics doesn't support re-entrant transactions")
  @Test
  public void testReEntTxn() throws InterruptedException,
      EventDeliveryException {

    Event event;
    Event event2;
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
    }
    transaction.commit();
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

  @Ignore("BasicChannelSemantics doesn't support re-entrant transactions")
  @Test
  public void testReEntTxnRollBack() throws InterruptedException,
      EventDeliveryException {
    Event event;
    Event event2;
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

}
