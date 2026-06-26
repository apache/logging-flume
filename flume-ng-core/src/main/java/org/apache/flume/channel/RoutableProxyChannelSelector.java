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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RoutableProxyChannelSelector extends LoadBalancingChannelSelector {
    private static final Logger LOGGER = LogManager.getLogger(RoutableProxyChannelSelector.class);
    private static final String HEADER_NAME = "headerName";
    private static final String SELECTOR = ".selector.";
    private static final String CHANNELS = "channels";
    private static final String DEFAULT = "default";
    private static final String TYPE = "type";

    private final Map<String, ChannelSelector> selectorMap = new HashMap<>();
    private ChannelSelector defaultSelector;
    private String headerName;

    public void addSelector(String headerName, ChannelSelector selector) {
        selectorMap.put(headerName, selector);
    }

    public void setDefaultSelector(ChannelSelector defaultSelector) {
        this.defaultSelector = defaultSelector;
    }

    public ChannelSelector getDefaultSelector() {
        return defaultSelector;
    }

    @Override
    public void configure(Context context) {
        Configurables.ensureRequiredNonNull(context, HEADER_NAME);
        List<Channel> allChannels = getAllChannels();
        for (Map.Entry<String, String> entry : context.getParameters().entrySet()) {
            if (entry.getKey().equals(HEADER_NAME)) {
                this.headerName = entry.getValue();
            } else if (!entry.getKey().equals(TYPE)) {
                String key = StringUtils.substringBefore(entry.getKey(), ".");
                Map<String, String> map = context.getSubProperties(key + SELECTOR);
                if (map != null) {
                    String channelNames = getRequiredNonNull(map, CHANNELS, key);
                    Set<String> channelSet = new HashSet<>(Arrays.asList(channelNames.split("\\s+")));
                    List<Channel> channels = new ArrayList<>(channelSet.size());
                    for (String channelName : channelSet) {
                        for (Channel channel : allChannels) {
                            if (channelName.equals(channel.getName())) {
                                channels.add(channel);
                            }
                        }
                    }
                    ChannelSelector selector = ChannelSelectorFactory.create(channels, map);
                    if (DEFAULT.equals(key)) {
                        defaultSelector = selector;
                    } else {
                        selectorMap.put(key, selector);
                    }
                }
            }
        }
        if (headerName == null) {
            throw new IllegalArgumentException("No header name specified for RoutableProxy");
        }
    }

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        ChannelSelector selector = getSelector(event);
        if (selector != null) {
            return selector.getRequiredChannels(event);
        }
        return new ArrayList<>();
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        ChannelSelector selector = getSelector(event);
        if (selector != null) {
            return selector.getOptionalChannels(event);
        }
        return new ArrayList<>();
    }

    private ChannelSelector getSelector(Event event) {
        ChannelSelector channelSelector = selectorMap.get(event.getHeaders().get(headerName));
        if (channelSelector == null) {
            channelSelector = defaultSelector;
        }
        return channelSelector;
    }

    private String getRequiredNonNull(Map<String, String> map, String keyName, String prefix) {
        String value = map.get(keyName);
        if (value == null) {
            throw new IllegalArgumentException(String.format("Missing key %s in %s", keyName, prefix));
        }
        return value;
    }
}
