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
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultChannelFactory implements ChannelFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultChannelFactory.class);

  private Map<String, Class<? extends Channel>> channelRegistry;

  public DefaultChannelFactory() {
    channelRegistry = new HashMap<String, Class<? extends Channel>>();
  }

  @Override
  public boolean register(String name, Class<? extends Channel> channelClass) {
    logger.info("Register channel name:{} class:{}", name, channelClass);

    if (channelRegistry.containsKey(name)) {
      return false;
    }

    channelRegistry.put(name, channelClass);
    return true;
  }

  @Override
  public boolean unregister(String name) {
    logger.info("Unregister channel class:{}", name);

    return channelRegistry.remove(name) != null;
  }

  @Override
  public Set<String> getChannelNames() {
    return channelRegistry.keySet();
  }

  @Override
  public Channel create(String name) throws InstantiationException {
    Preconditions.checkNotNull(name);

    logger.debug("Creating instance of channel {}", name);

    if (!channelRegistry.containsKey(name)) {
      return null;
    }

    Channel channel = null;

    try {
      channel = channelRegistry.get(name).newInstance();
    } catch (IllegalAccessException e) {
      throw new InstantiationException("Unable to create channel " + name
          + " due to " + e.getMessage());
    }

    return channel;
  }

  @Override
  public String toString() {
    return "{ channelRegistry:" + channelRegistry + " }";
  }

  public Map<String, Class<? extends Channel>> getChannelRegistry() {
    return channelRegistry;
  }

  public void setChannelRegistry(
      Map<String, Class<? extends Channel>> channelRegistry) {
    this.channelRegistry = channelRegistry;
  }

  // build a fanout channel from the list of channel names and map of <name,channels>
  @Override
  public Channel createFanout(String chList, Map<String, Channel> chMap)
      throws InstantiationException {

    FanoutChannel fnc = (FanoutChannel)create("fanout");
    StringTokenizer tk = new StringTokenizer(chList, ",");
    while (tk.hasMoreTokens()) {
      fnc.addFanout(chMap.get(tk.nextToken()));
    }
    return fnc;
  }

}
