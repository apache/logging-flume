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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.fest.reflect.core.Reflection.*;


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
    transaction.close();

    transaction = channel.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    Event event2 = channel.take();
    Assert.assertEquals(event, event2);
    transaction.commit();
  }

  @Test
  public void testChannelResize() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "5");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for(int i=0; i < 5; i++) {
      channel.put(EventBuilder.withBody(String.format("test event %d", i).getBytes()));
    }
    transaction.commit();
    transaction.close();

    /*
     * Verify overflow semantics
     */
    transaction = channel.getTransaction();
    boolean overflowed = false;
    try {
      transaction.begin();
      channel.put(EventBuilder.withBody("overflow event".getBytes()));
      transaction.commit();
    } catch (ChannelException e) {
      overflowed = true;
      transaction.rollback();
    } finally {
      transaction.close();
    }
    Assert.assertTrue(overflowed);

    /*
     * Reconfigure capacity down and add another event, shouldn't result in exception
     */
    parms.put("capacity", "6");
    context.putAll(parms);
    Configurables.configure(channel, context);
    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("extended capacity event".getBytes()));
    transaction.commit();
    transaction.close();

    /*
     * Attempt to reconfigure capacity to below current entry count and verify
     * it wasn't carried out
     */
    parms.put("capacity", "2");
    parms.put("transactionCapacity", "2");
    context.putAll(parms);
    Configurables.configure(channel, context);
    for(int i=0; i < 6; i++) {
      transaction = channel.getTransaction();
      transaction.begin();
      Assert.assertNotNull(channel.take());
      transaction.commit();
      transaction.close();
    }
  }

  @Test(expected=ChannelException.class)
  public void testTransactionPutCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "2");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // shouldn't be able to fit a third in the buffer
    channel.put(EventBuilder.withBody("test".getBytes()));
    Assert.fail();
  }

  @Test(expected=ChannelException.class)
  public void testCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "5");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    // this should kill  it
    transaction.commit();
    Assert.fail();
  }

  @Test
  public void testCapacityBufferEmptyingAfterTakeCommit() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test
  public void testCapacityBufferEmptyingAfterRollback() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "3");
    parms.put("transactionCapacity", "3");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.rollback();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
  }

  @Test(expected=ChannelException.class)
  public void testByteCapacityOverload() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    transaction.commit();
    transaction.close();

    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    // this should kill  it
    transaction.commit();
    Assert.fail();

  }

  public void testByteCapacityBufferEmptyingAfterTakeCommit() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      throw new RuntimeException("Put was able to overflow byte capacity.");
    } catch (ChannelException ce)
    {
      //Do nothing
    }

    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    channel.take();
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      throw new RuntimeException("Put was able to overflow byte capacity.");
    } catch (ChannelException ce)
    {
      //Do nothing
    }
    tx.commit();
    tx.close();
  }

  @Test
  public void testByteCapacityBufferEmptyingAfterRollback() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    tx.rollback();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
  }

  @Test
  public void testByteCapacityBufferChangeConfig() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    byte[] eventBody = new byte[405];

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
    channel.stop();
    parms.put("byteCapacity", "1500");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    try {
      channel.put(EventBuilder.withBody(eventBody));
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }

    channel.stop();
    parms.put("byteCapacity", "250");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(eventBody));
    tx.commit();
    tx.close();
    channel.stop();

    parms.put("byteCapacity", "300");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();
    try {
      for(int i = 0; i < 2; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }

    channel.stop();
    parms.put("byteCapacity", "3300");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();

    try {
      for(int i = 0; i < 15; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }
    channel.stop();
    parms.put("byteCapacity", "4000");
    context.putAll(parms);
    Configurables.configure(channel,  context);
    channel.start();
    tx = channel.getTransaction();
    tx.begin();

    try {
      for(int i = 0; i < 25; i++) {
        channel.put(EventBuilder.withBody(eventBody));
      }
      tx.commit();
      Assert.fail();
    } catch ( ChannelException e ) {
      //success
      tx.rollback();
    } finally {
      tx.close();
    }
    channel.stop();
  }

  /*
   * This would cause a NPE without FLUME-1622.
   */
  @Test
  public void testNullEmptyEvent() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("byteCapacity", "2000");
    parms.put("byteCapacityBufferPercentage", "20");
    context.putAll(parms);
    Configurables.configure(channel,  context);

    Transaction tx = channel.getTransaction();
    tx.begin();
    //This line would cause a NPE without FLUME-1622.
    channel.put(EventBuilder.withBody(null));
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody(new byte[0]));
    tx.commit();
    tx.close();


  }

  @Test
  public void testNegativeCapacities() {
    Context context = new Context();
    Map<String, String> parms = new HashMap<String, String>();
    parms.put("capacity", "-3");
    parms.put("transactionCapacity", "-1");
    context.putAll(parms);
    Configurables.configure(channel, context);

    Assert.assertTrue(field("queue")
            .ofType(LinkedBlockingDeque.class)
            .in(channel).get()
            .remainingCapacity() > 0);

    Assert.assertTrue(field("transCapacity")
            .ofType(Integer.class)
            .in(channel).get() > 0);
  }
}
