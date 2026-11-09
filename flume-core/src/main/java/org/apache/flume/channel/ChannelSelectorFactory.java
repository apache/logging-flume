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

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import java.util.List;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.ComponentWithClassName;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.channel.ChannelSelectorConfiguration;
import org.apache.flume.conf.channel.ChannelSelectorType;
import org.apache.flume.spi.ChannelSelectorProvider;
import org.apache.flume.spi.ComponentRegistry;

@ServiceConsumer(
        value = ChannelSelectorProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public class ChannelSelectorFactory {

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<ChannelSelector> REGISTRY = new ComponentRegistry<>(
            ChannelSelectorProvider.class,
            ChannelSelector.class,
            "channel selector",
            type -> ComponentWithClassName.getClassName(ChannelSelectorType.class, type));

    public static ChannelSelector create(List<Channel> channels, Map<String, String> config) {

        ChannelSelector selector = getSelectorForType(config.get(BasicConfigurationConstants.CONFIG_TYPE));

        selector.setChannels(channels);

        Context context = new Context();
        context.putAll(config);

        Configurables.configure(selector, context);
        return selector;
    }

    public static ChannelSelector create(List<Channel> channels, ChannelSelectorConfiguration conf) {
        String type = ChannelSelectorType.REPLICATING.toString();
        if (conf != null) {
            type = conf.getType();
        }
        ChannelSelector selector = getSelectorForType(type);
        selector.setChannels(channels);
        Configurables.configure(selector, conf);
        return selector;
    }

    private static ChannelSelector getSelectorForType(String type) {
        if (type == null || type.trim().length() == 0) {
            return new ReplicatingChannelSelector();
        }
        return REGISTRY.newInstance(type);
    }
}
