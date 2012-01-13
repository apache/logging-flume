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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultChannelFactory implements ChannelFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultChannelFactory.class);

  private Map<Class<?>, Map<String, Channel>> channels;

  public DefaultChannelFactory() {
    channels = new HashMap<Class<?>, Map<String, Channel>>();
  }

  @Override
  public boolean unregister(Channel channel) {
    Preconditions.checkNotNull(channel);
    logger.info("Unregister channel {}", channel);
    boolean removed = false;

    Map<String, Channel> channelMap = channels.get(channel.getClass());
    if (channelMap != null) {
      removed = (channelMap.remove(channel.getName()) != null);

      if (channelMap.size() == 0) {
        channels.remove(channel.getClass());
      }
    }

    return removed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Channel create(String name, String type) throws FlumeException {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);
    logger.debug("Creating instance of channel {} type {}", name, type);

    String channelClassName = type;

    ChannelType channelType = ChannelType.OTHER;
    try {
      channelType = ChannelType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException ex) {
      logger.debug("Channel type {} is a custom type", type);
    }

    if (!channelType.equals(ChannelType.OTHER)) {
      channelClassName = channelType.getChannelClassName();
    }

    Class<? extends Channel> channelClass = null;
    try {
      channelClass = (Class<? extends Channel>) Class.forName(channelClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load channel type: " + type
          + ", class: " + channelClassName, ex);
    }

    Map<String, Channel> channelMap = channels.get(channelClass);
    if (channelMap == null) {
      channelMap = new HashMap<String, Channel>();
      channels.put(channelClass, channelMap);
    }

    Channel channel = channelMap.get(name);

    if (channel == null) {
      try {
        channel = channelClass.newInstance();
        channel.setName(name);
        channelMap.put(name, channel);
      } catch (Exception ex) {
        // Clean up channel map
        channels.remove(channelClass);
        throw new FlumeException("Unable to create channel: " + name
            + ", type: " + type + ", class: " + channelClassName, ex);
      }
    }

    return channel;
  }

  public Map<Class<?>, Map<String, Channel>> getRegistryClone() {
    Map<Class<?>, Map<String, Channel>> result =
        new HashMap<Class<?>, Map<String, Channel>>();

    for (Class<?> klass : channels.keySet()) {
      Map<String, Channel> channelMap = channels.get(klass);
      Map<String, Channel> resultChannelMap = new HashMap<String, Channel>();
      resultChannelMap.putAll(channelMap);
      result.put(klass, resultChannelMap);
    }

    return result;
  }
}
