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
  public static final String CONFIG_PREFIX_MAPPING = "mapping";

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
      .getLogger(MultiplexingChannelSelector.class);

  private static final List<Channel> EMPTY_LIST =
      Collections.emptyList();

  private String headerName;

  private Map<String, List<Channel>> channelMapping;

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    String headerValue = event.getHeaders().get(headerName);
    if (headerValue == null || headerValue.trim().length() == 0) {
      return EMPTY_LIST;
    }

    List<Channel> channels = channelMapping.get(headerValue);

    if (channels == null) {
      channels = EMPTY_LIST;
    }

    return channels;
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return EMPTY_LIST;
  }

  @Override
  public void configure(Context context) {
    this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
        DEFAULT_MULTIPLEX_HEADER);

    Map<String, Object> mapConfig = context.getSubProperties(
        CONFIG_PREFIX_MAPPING + ".");

    Map<String, Channel> channelNameMap = new HashMap<String, Channel>();

    for (Channel ch : getAllChannels()) {
      channelNameMap.put(ch.getName(), ch);
    }

    channelMapping = new HashMap<String, List<Channel>>();

    for (String headerValue : mapConfig.keySet()) {
      String[] chNames = ((String) mapConfig.get(headerValue)).split(" ");
      List<Channel> configuredChannels = new ArrayList<Channel>();
      for (String name : chNames) {
        Channel ch = channelNameMap.get(name);
        if (ch != null) {
          configuredChannels.add(ch);
        } else {
          throw new FlumeException("Selector channel not found: "
              + name);
        }
      }

      if (configuredChannels.size() == 0) {
        throw new FlumeException("No channel configured for when "
            + "header value is: " + headerValue);
      }

      if (channelMapping.put(headerValue, configuredChannels) != null) {
        throw new FlumeException("Selector channel configured twice");
      }
    }

    if (channelMapping.size() == 0) {
      throw new FlumeException("No mapping configured for selector");
    }
  }

}
