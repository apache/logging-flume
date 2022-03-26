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
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.channel.ChannelSelectorType;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class TestLoadBalancingChannelSelector {

  private List<Channel> channels = new ArrayList<Channel>();

  private ChannelSelector selector;

  @Before
  public void setUp() throws Exception {
    channels.clear();
    channels.add(MockChannel.createMockChannel("ch1"));
    channels.add(MockChannel.createMockChannel("ch2"));
    channels.add(MockChannel.createMockChannel("ch3"));
    channels.add(MockChannel.createMockChannel("ch4"));
    Map<String, String> config = new HashMap<>();
    config.put(BasicConfigurationConstants.CONFIG_TYPE, ChannelSelectorType.LOAD_BALANCING.name());
    selector = ChannelSelectorFactory.create(channels, config);
  }

  @Test
  public void testLoadBalancingSelector() throws Exception {
    selector.configure(new Context());
    validateChannel(selector, "ch1");
    validateChannel(selector, "ch2");
    validateChannel(selector, "ch3");
    validateChannel(selector, "ch4");

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertEquals(0, optCh.size());
  }

  private void validateChannel(ChannelSelector selector, String channelName) {
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertEquals(1, channels.size());
    Assert.assertEquals(channelName, channels.get(0).getName());
  }
}
