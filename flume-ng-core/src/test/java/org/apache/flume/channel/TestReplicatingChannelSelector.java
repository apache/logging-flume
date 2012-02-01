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

    selector = ChannelSelectorFactory.create(
        channels, new HashMap<String, String>());
  }

  @Test
  public void testReplicatingSelector() throws Exception {
    List<Channel> channels = selector.getRequiredChannels(new MockEvent());
    Assert.assertNotNull(channels);
    Assert.assertTrue(channels.size() == 3);
    Assert.assertTrue(channels.get(0).getName().equals("ch1"));
    Assert.assertTrue(channels.get(1).getName().equals("ch2"));
    Assert.assertTrue(channels.get(2).getName().equals("ch3"));

    List<Channel> optCh = selector.getOptionalChannels(new MockEvent());
    Assert.assertTrue(optCh.size() == 0);
  }

}
