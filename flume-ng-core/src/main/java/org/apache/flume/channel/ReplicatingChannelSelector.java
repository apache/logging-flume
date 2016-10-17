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
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * Replicating channel selector. This selector allows the event to be placed
 * in all the channels that the source is configured with.
 */
public class ReplicatingChannelSelector extends AbstractChannelSelector {

  /**
   * Configuration to set a subset of the channels as optional.
   */
  public static final String CONFIG_OPTIONAL = "optional";
  List<Channel> requiredChannels = null;
  List<Channel> optionalChannels = new ArrayList<Channel>();

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    /*
     * Seems like there are lot of components within flume that do not call
     * configure method. It is conceiveable that custom component tests too
     * do that. So in that case, revert to old behavior.
     */
    if (requiredChannels == null) {
      return getAllChannels();
    }
    return requiredChannels;
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return optionalChannels;
  }

  @Override
  public void configure(Context context) {
    String optionalList = context.getString(CONFIG_OPTIONAL);
    requiredChannels = new ArrayList<Channel>(getAllChannels());
    Map<String, Channel> channelNameMap = getChannelNameMap();
    if (optionalList != null && !optionalList.isEmpty()) {
      for (String optional : optionalList.split("\\s+")) {
        Channel optionalChannel = channelNameMap.get(optional);
        requiredChannels.remove(optionalChannel);
        if (!optionalChannels.contains(optionalChannel)) {
          optionalChannels.add(optionalChannel);
        }
      }
    }
  }
}
