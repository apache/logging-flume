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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.channel.ChannelSelectorConfiguration;
import org.apache.flume.conf.channel.ChannelSelectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelSelectorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      ChannelSelectorFactory.class);

  public static ChannelSelector create(List<Channel> channels,
      Map<String, String> config) {

    ChannelSelector selector = getSelectorForType(config.get(
        BasicConfigurationConstants.CONFIG_TYPE));

    selector.setChannels(channels);

    Context context = new Context();
    context.putAll(config);

    Configurables.configure(selector, context);
    return selector;
  }

  public static ChannelSelector create(List<Channel> channels,
      ChannelSelectorConfiguration conf) {
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

    String selectorClassName = type;
    ChannelSelectorType  selectorType = ChannelSelectorType.OTHER;

    try {
      selectorType = ChannelSelectorType.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      LOGGER.debug("Selector type {} is a custom type", type);
    }

    if (!selectorType.equals(ChannelSelectorType.OTHER)) {
      selectorClassName = selectorType.getChannelSelectorClassName();
    }

    ChannelSelector selector = null;

    try {
      @SuppressWarnings("unchecked")
      Class<? extends ChannelSelector> selectorClass =
          (Class<? extends ChannelSelector>) Class.forName(selectorClassName);
      selector = selectorClass.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to load selector type: " + type
          + ", class: " + selectorClassName, ex);
    }

    return selector;
  }

}
