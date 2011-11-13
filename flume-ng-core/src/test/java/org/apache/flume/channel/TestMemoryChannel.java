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
