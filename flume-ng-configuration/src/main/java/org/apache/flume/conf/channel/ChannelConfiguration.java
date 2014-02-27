/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.conf.channel;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.source.SourceConfiguration;

public class ChannelConfiguration extends ComponentConfiguration {

  public ChannelConfiguration(String componentName) {
    super(componentName);
  }

  @Override
  public void configure(Context context) throws ConfigurationException {
    super.configure(context);
  }

  public enum ChannelConfigurationType {
    OTHER(null),
    MEMORY("org.apache.flume.conf.channel.MemoryChannelConfiguration"),
    
    /**
     * File channel
     */
    FILE("org.apache.flume.conf.channel.FileChannelConfiguration"),

    /**
     * JDBC channel provided by org.apache.flume.channel.jdbc.JdbcChannel
     */
    JDBC("org.apache.flume.conf.channel.JdbcChannelConfiguration"),

    /**
     * Spillable Memory channel
     */
    SPILLABLEMEMORY("org.apache.flume.conf.channel.SpillableMemoryChannelConfiguration");

    private String channelConfigurationType;

    private ChannelConfigurationType(String type) {
      this.channelConfigurationType = type;

    }

    public String getChannelConfigurationType() {
      return channelConfigurationType;
    }

    @SuppressWarnings("unchecked")
    public ChannelConfiguration getConfiguration(String name)
        throws ConfigurationException {
      if (this.equals(ChannelConfigurationType.OTHER)) {
        return new ChannelConfiguration(name);
      }
      Class<? extends ChannelConfiguration> clazz;
      ChannelConfiguration instance = null;
      try {
        if (channelConfigurationType != null) {
          clazz =
              (Class<? extends ChannelConfiguration>) Class
                  .forName(channelConfigurationType);
          instance = clazz.getConstructor(String.class).newInstance(name);
        } else {
          return new ChannelConfiguration(name);
        }
      } catch (ClassNotFoundException e) {
        // Could not find the configuration stub, do basic validation
        instance = new ChannelConfiguration(name);
        // Let the caller know that this was created because of this exception.
        instance.setNotFoundConfigClass();
      } catch (Exception e) {
        throw new ConfigurationException(e);
      }
      return instance;
    }
  }

}
