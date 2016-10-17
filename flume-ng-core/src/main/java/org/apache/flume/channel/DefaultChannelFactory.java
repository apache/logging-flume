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

import java.util.Locale;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.channel.ChannelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultChannelFactory implements ChannelFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultChannelFactory.class);

  @Override
  public Channel create(String name, String type) throws FlumeException {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(type, "type");
    logger.info("Creating instance of channel {} type {}", name, type);
    Class<? extends Channel> channelClass = getClass(type);
    try {
      return channelClass.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create channel: " + name
          + ", type: " + type + ", class: " + channelClass.getName(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Channel> getClass(String type) throws FlumeException {
    String channelClassName = type;
    ChannelType channelType = ChannelType.OTHER;
    try {
      channelType = ChannelType.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      logger.debug("Channel type {} is a custom type", type);
    }
    if (!channelType.equals(ChannelType.OTHER)) {
      channelClassName = channelType.getChannelClassName();
    }
    try {
      return (Class<? extends Channel>) Class.forName(channelClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load channel type: " + type
          + ", class: " + channelClassName, ex);
    }
  }
}
