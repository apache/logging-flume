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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiplexingChannelSelector extends AbstractChannelSelector {

  public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
  public static final String DEFAULT_MULTIPLEX_HEADER =
      "flume.selector.header";
  public static final String CONFIG_PREFIX_MAPPING = "mapping.";
  public static final String CONFIG_DEFAULT_CHANNEL = "default";
  public static final String CONFIG_PREFIX_OPTIONAL = "optional";

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
    .getLogger(MultiplexingChannelSelector.class);

  private static final List<Channel> EMPTY_LIST =
      Collections.emptyList();

  private String headerName;

  private Map<String, List<Channel>> channelMapping;
  private Map<String, List<Channel>> optionalChannels;
  private List<Channel> defaultChannels;
  @Override
  public List<Channel> getRequiredChannels(Event event) {
    String headerValue = event.getHeaders().get(headerName);
    if (headerValue == null || headerValue.trim().length() == 0) {
      return defaultChannels;
    }

    List<Channel> channels = channelMapping.get(headerValue);

    //This header value does not point to anything
    //Return default channel(s) here.
    if (channels == null) {
      channels = defaultChannels;
    }

    return channels;
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    String hdr = event.getHeaders().get(headerName);
    List<Channel> channels = optionalChannels.get(hdr);

    if(channels == null) {
      channels = EMPTY_LIST;
    }
    return channels;
  }

  @Override
  public void configure(Context context) {
    this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
        DEFAULT_MULTIPLEX_HEADER);

    Map<String, Channel> channelNameMap = getChannelNameMap();

    defaultChannels = getChannelListFromNames(
        context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);

    Map<String, String> mapConfig =
        context.getSubProperties(CONFIG_PREFIX_MAPPING);

    channelMapping = new HashMap<String, List<Channel>>();

    for (String headerValue : mapConfig.keySet()) {
      List<Channel> configuredChannels = getChannelListFromNames(
          mapConfig.get(headerValue),
          channelNameMap);

      //This should not go to default channel(s)
      //because this seems to be a bad way to configure.
      if (configuredChannels.size() == 0) {
        throw new FlumeException("No channel configured for when "
            + "header value is: " + headerValue);
      }

      if (channelMapping.put(headerValue, configuredChannels) != null) {
        throw new FlumeException("Selector channel configured twice");
      }
    }
    //If no mapping is configured, it is ok.
    //All events will go to the default channel(s).
    Map<String, String> optionalChannelsMapping =
        context.getSubProperties(CONFIG_PREFIX_OPTIONAL + ".");

    optionalChannels = new HashMap<String, List<Channel>>();
    for (String hdr : optionalChannelsMapping.keySet()) {
      List<Channel> confChannels = getChannelListFromNames(
              optionalChannelsMapping.get(hdr), channelNameMap);
      if (confChannels.isEmpty()) {
        confChannels = EMPTY_LIST;
      }
      //Remove channels from optional channels, which are already
      //configured to be required channels.

      List<Channel> reqdChannels = channelMapping.get(hdr);
      //Check if there are required channels, else defaults to default channels
      if(reqdChannels == null || reqdChannels.isEmpty()) {
        reqdChannels = defaultChannels;
      }
      for (Channel c : reqdChannels) {
        if (confChannels.contains(c)) {
          confChannels.remove(c);
        }
      }

      if (optionalChannels.put(hdr, confChannels) != null) {
        throw new FlumeException("Selector channel configured twice");
      }
    }

  }

}
