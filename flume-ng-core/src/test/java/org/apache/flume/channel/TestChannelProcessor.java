/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.channel;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestChannelProcessor {

  /**
   * Ensure that we bubble up any specific exception thrown from getTransaction
   * instead of another exception masking it such as an NPE
   */
  @Test(expected = ChannelException.class)
  public void testExceptionFromGetTransaction() {
    // create a channel which unexpectedly throws a ChEx on getTransaction()
    Channel ch = mock(Channel.class);
    when(ch.getTransaction()).thenThrow(new ChannelException("doh!"));

    ChannelSelector sel = new ReplicatingChannelSelector();
    sel.setChannels(Lists.newArrayList(ch));
    ChannelProcessor proc = new ChannelProcessor(sel);

    List<Event> events = Lists.newArrayList();
    events.add(EventBuilder.withBody("event 1", Charsets.UTF_8));

    proc.processEventBatch(events);
  }

  /**
   * Ensure that we see the original NPE from the PreConditions check instead
   * of an auto-generated NPE, which could be masking something else.
   */
  @Test
  public void testNullFromGetTransaction() {
    // channel which returns null from getTransaction()
    Channel ch = mock(Channel.class);
    when(ch.getTransaction()).thenReturn(null);

    ChannelSelector sel = new ReplicatingChannelSelector();
    sel.setChannels(Lists.newArrayList(ch));
    ChannelProcessor proc = new ChannelProcessor(sel);

    List<Event> events = Lists.newArrayList();
    events.add(EventBuilder.withBody("event 1", Charsets.UTF_8));

    boolean threw = false;
    try {
      proc.processEventBatch(events);
    } catch (NullPointerException ex) {
      threw = true;
      Assert.assertNotNull("NPE must be manually thrown", ex.getMessage());
    }
    Assert.assertTrue("Must throw NPE", threw);
  }

  /*
 * Test delivery to optional and required channels
 * Test both processEvent and processEventBatch
 */
  @Test
  public void testRequiredAndOptionalChannels() {
    Context context = new Context();
    ArrayList<Channel> channels = new ArrayList<Channel>();
    for (int i = 0; i < 4; i++) {
      Channel ch = new MemoryChannel();
      ch.setName("ch" + i);
      Configurables.configure(ch, context);
      channels.add(ch);
    }

    ChannelSelector selector = new ReplicatingChannelSelector();
    selector.setChannels(channels);

    context = new Context();
    context.put(ReplicatingChannelSelector.CONFIG_OPTIONAL, "ch2 ch3");
    Configurables.configure(selector, context);

    ChannelProcessor processor = new ChannelProcessor(selector);
    context = new Context();
    Configurables.configure(processor, context);


    Event event1 = EventBuilder.withBody("event 1", Charsets.UTF_8);
    processor.processEvent(event1);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    }

    for (Channel channel : channels) {
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      Event event_ch = channel.take();
      Assert.assertEquals(event1, event_ch);
      transaction.commit();
      transaction.close();
    }

    List<Event> events = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      events.add(EventBuilder.withBody("event " + i, Charsets.UTF_8));
    }
    processor.processEventBatch(events);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    }
    for (Channel channel : channels) {
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      for (int i = 0; i < 100; i++) {
        Event event_ch = channel.take();
        Assert.assertNotNull(event_ch);
      }
      transaction.commit();
      transaction.close();
    }
  }

}
