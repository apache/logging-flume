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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFanoutChannel {

  private final Logger logger = LoggerFactory.getLogger(TestFanoutChannel.class);

  @Before
  public void setUp() {
  }

  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {

    Channel ch1 = getMemChannel(20);
    Channel ch2 = getMemChannel(20);
    FanoutChannel chFan = new FanoutChannel();
    chFan.addFanout(ch1);
    chFan.addFanout(ch2);

    // write data into fanout channel and commit
    putIntoFanoutChannel(chFan, true);
    // verify the data in each channel
    verifyChannelData(ch1);
    verifyChannelData(ch2);

    // write data into fanout channel and rollback
    putIntoFanoutChannel(chFan, false);
    // verify that channels have no events
    verifyChannelEmpty(ch1);
    verifyChannelEmpty(ch2);
  }

  @Test
  public void testPutErrors() throws InterruptedException, EventDeliveryException {

    Channel ch1 = getMemChannel(20);
    Channel ch2 = getMemChannel(5); // smaller capacity queue
    FanoutChannel chFan = new FanoutChannel();
    chFan.addFanout(ch1);
    chFan.addFanout(ch2);

    // write data into fanout channel and commit
    // it should run into error due to small channel overflow
    try {
    putIntoFanoutChannel(chFan, true);
    } catch (ChannelException e) {
      logger.warn("small channel overflow", e);
    }

    //verify that channels are empty as we rolled back due to error
    verifyChannelEmpty(ch1);
    verifyChannelEmpty(ch2);
  }

  private Channel getMemChannel(int Capacity) {
    Channel ch1 = new MemoryChannel();
    Context context1 = new Context();
    context1.put("keep-alive", "1");
    context1.put("capacity", Integer.toString(Capacity));
    Configurables.configure(ch1, context1);
    return ch1;
  }

  private void putIntoFanoutChannel(Channel chFan, boolean commitData) {
    Event event;
    int putCounter = 0;

    Transaction transaction = chFan.getTransaction();
    Assert.assertNotNull(transaction);

    try {
      transaction.begin();
      for (putCounter = 0; putCounter < 10; putCounter++) {
        event = EventBuilder.withBody(("test event" + putCounter).getBytes());
        chFan.put(event);
      }
      if (commitData == true)
        transaction.commit();
      else
        transaction.rollback();
    } finally {
      transaction.close();
    }
  }

  private void verifyChannelData(Channel ch) {
     Event event2;

    Transaction transaction = ch.getTransaction();
    Assert.assertNotNull(transaction);

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      event2 = ch.take();
      Assert.assertNotNull("lost an event", event2);
      Assert.assertArrayEquals(event2.getBody(), ("test event" + i).getBytes());
    }
    event2 = ch.take();
    Assert.assertNull("extra event found", event2);
    transaction.commit();
    transaction.close();
  }
  
  private void verifyChannelEmpty(Channel ch) {
    Event event2;
    
   Transaction transaction = ch.getTransaction();
   Assert.assertNotNull(transaction);

   transaction.begin();
   event2 = ch.take();
   Assert.assertNull("extra event found", event2);
   transaction.commit();
   transaction.close();
  }
}
