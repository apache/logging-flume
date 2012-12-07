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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.junit.Before;
import org.junit.Test;

public class TestReplicatingChannelSelector {

  private List<Channel> channels = new ArrayList<Channel>();

  private ChannelSelector selector;

  @Before
  public void setUp() throws Exception {
    channels.clear();
    channels.add(MockChannel.createMockChannel("ch1"));
    channels.add(MockChannel.createMockChannel("ch2"));
    channels.add(MockChannel.createMockChannel("ch3"));
    channels.add(MockChannel.createMockChannel("ch4"));
    selector = ChannelSelectorFactory.create(
        channels, new HashMap<String, String>());
  }

  @Test
  public void testReplicatingSelector() throws Exception {
    selector.configure(new Context());
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertEquals(4, channels.size());
    Assert.assertEquals("ch1", channels.get(0).getName());
    Assert.assertEquals("ch2", channels.get(1).getName());
    Assert.assertEquals("ch3", channels.get(2).getName());
    Assert.assertEquals("ch4", channels.get(3).getName());

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertEquals(0, optCh.size());
  }

  @Test
  public void testOptionalChannels() throws Exception {
    Context context = new Context();
    context.put(ReplicatingChannelSelector.CONFIG_OPTIONAL, "ch1");
    Configurables.configure(selector, context);
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertEquals(3, channels.size());
    Assert.assertEquals("ch2", channels.get(0).getName());
    Assert.assertEquals("ch3", channels.get(1).getName());
    Assert.assertEquals("ch4", channels.get(2).getName());

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertEquals(1, optCh.size());
    Assert.assertEquals("ch1", optCh.get(0).getName());

  }


  @Test
  public void testMultipleOptionalChannels() throws Exception {
    Context context = new Context();
    context.put(ReplicatingChannelSelector.CONFIG_OPTIONAL, "ch1 ch4");
    Configurables.configure(selector, context);
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertEquals(2, channels.size());
    Assert.assertEquals("ch2", channels.get(0).getName());
    Assert.assertEquals("ch3", channels.get(1).getName());

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertEquals(2, optCh.size());
    Assert.assertEquals("ch1", optCh.get(0).getName());
    Assert.assertEquals("ch4", optCh.get(1).getName());
  }

  @Test
  public void testMultipleOptionalChannelsSameChannelTwice() throws Exception {
    Context context = new Context();
    context.put(ReplicatingChannelSelector.CONFIG_OPTIONAL, "ch1 ch4 ch1");
    Configurables.configure(selector, context);
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertEquals(2, channels.size());
    Assert.assertEquals("ch2", channels.get(0).getName());
    Assert.assertEquals("ch3", channels.get(1).getName());

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertEquals(2, optCh.size());
    Assert.assertEquals("ch1", optCh.get(0).getName());
    Assert.assertEquals("ch4", optCh.get(1).getName());
  }
}
