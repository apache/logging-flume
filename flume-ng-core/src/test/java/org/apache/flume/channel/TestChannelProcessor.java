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
import java.util.List;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.*;

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

}
