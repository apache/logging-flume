/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import junit.framework.Assert;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

public class TestRoutableProxyChannelSelector {

    private List<Channel> channels = new ArrayList<Channel>();
    private static final String[] config = new String[] {
        "headerName = processingMode",
        "type = routable_proxy",
        "normal.selector.type = load_balancing",
        "normal.selector.channels = ch1 ch2",
        "normal.selector.policy = round_robin",
        "default.selector.type = load_balancing",
        "default.selector.channels = ch3 ch4",
        "default.selector.policy = round_robin"
    };

    private ChannelSelector selector;

    @Test
    public void testProxySelector() throws Exception {
        channels.clear();
        channels.add(MockChannel.createMockChannel("ch1"));
        channels.add(MockChannel.createMockChannel("ch2"));
        channels.add(MockChannel.createMockChannel("ch3"));
        channels.add(MockChannel.createMockChannel("ch4"));
        RoutableProxyChannelSelector selector =
                (RoutableProxyChannelSelector) ChannelSelectorFactory.create(channels, getConfig());
        Assert.assertNotNull(selector);
        Assert.assertNotNull(selector.getDefaultSelector());
        Event event = new SimpleEvent();
        event.getHeaders().put("processingMode", "normal");
        List<Channel> channels = selector.getRequiredChannels(event);
        Assert.assertNotNull(channels);
        Assert.assertEquals(1, channels.size());
        String channelName = channels.get(0).getName();
        Assert.assertTrue(channelName.equals("ch1") || channelName.equals("ch2"));
    }

    @Test
    public void testProxySelectorManualConfig() throws Exception {
        channels.clear();
        channels.add(MockChannel.createMockChannel("ch1"));
        channels.add(MockChannel.createMockChannel("ch2"));
        channels.add(MockChannel.createMockChannel("ch3"));
        channels.add(MockChannel.createMockChannel("ch4"));
        Map<String, String> config = new HashMap<>();
        config.put("headerName", "processingMode");
        config.put("type", "routable_proxy");
        RoutableProxyChannelSelector selector =
                (RoutableProxyChannelSelector) ChannelSelectorFactory.create(channels, config);
        config.clear();
        config.put("policy", "round_robin");
        config.put("type", "load_balancing");
        List<Channel> channels1 = new ArrayList<>();
        channels1.add(channels.get(0));
        channels1.add(channels.get(1));
        LoadBalancingChannelSelector loadBalancingSelector =
                (LoadBalancingChannelSelector) ChannelSelectorFactory.create(channels1, config);
        selector.setDefaultSelector(loadBalancingSelector);
        List<Channel> channels2 = new ArrayList<>();
        channels2.add(channels.get(2));
        channels2.add(channels.get(3));
        loadBalancingSelector = (LoadBalancingChannelSelector) ChannelSelectorFactory.create(channels2, config);
        selector.addSelector("validation", loadBalancingSelector);
        Assert.assertNotNull(selector);
        Assert.assertNotNull(selector.getDefaultSelector());
        Event event = new SimpleEvent();
        event.getHeaders().put("processingMode", "normal");
        List<Channel> channels = selector.getRequiredChannels(event);
        Assert.assertNotNull(channels);
        Assert.assertEquals(1, channels.size());
        String channelName = channels.get(0).getName();
        Assert.assertTrue(channelName.equals("ch1") || channelName.equals("ch2"));
    }

    private Map<String, String> getConfig() {
        return Arrays.stream(config)
                .map(line -> line.split("=", 2)) // Limit to 2 parts to keep values with '=' intact
                .filter(parts -> parts.length == 2)
                .collect(Collectors.toMap(parts -> parts[0].trim(), parts -> parts[1].trim()));
    }
}
