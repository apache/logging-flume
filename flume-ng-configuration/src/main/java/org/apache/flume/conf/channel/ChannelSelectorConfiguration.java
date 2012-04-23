/*
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

import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.channel.ChannelConfiguration.ChannelConfigurationType;
import org.apache.flume.conf.source.SourceConfiguration;

public class ChannelSelectorConfiguration extends
    ComponentConfiguration {

  protected Set<String> channelNames;

  protected ChannelSelectorConfiguration(String componentName) {
    super(componentName);
    // unless it is set to some other type
    this.setType(ChannelSelectorType.REPLICATING.toString());
    channelNames = null;
  }

  public Set<String> getChannels() {
    return channelNames;
  }

  public void setChannels(Set<String> channelNames) {
    this.channelNames = channelNames;
  }

  public void configure(Context context) throws ConfigurationException {
    super.configure(context);
  }

  public enum ChannelSelectorConfigurationType {
    OTHER(null),
    REPLICATING(null),
    MULTIPLEXING(
        "org.apache.flume.conf.channel." +
            "MultiplexingChannelSelectorConfiguration");

    private String selectorType;

    private ChannelSelectorConfigurationType(String type) {
      this.selectorType = type;
    }

    public String getChannelSelectorConfigurationType() {
      return this.selectorType;
    }

    @SuppressWarnings("unchecked")
    public ChannelSelectorConfiguration getConfiguration(
        String name)
        throws ConfigurationException {
      if (this.equals(ChannelConfigurationType.OTHER)) {
        return new ChannelSelectorConfiguration(name);
      }
      Class<? extends ChannelSelectorConfiguration> clazz;
      ChannelSelectorConfiguration instance = null;
      try {
        // Components where it is null, no configuration is necessary.
        if (this.selectorType != null) {
          clazz =
              (Class<? extends ChannelSelectorConfiguration>) Class
                  .forName(this.selectorType);
          instance = clazz.getConstructor(String.class).newInstance(name);
        } else {
          return new ChannelSelectorConfiguration(name);
        }
      } catch (ClassNotFoundException e) {
        // Could not find the configuration stub, do basic validation
        instance = new ChannelSelectorConfiguration(name);
        // Let the caller know that this was created because of this exception.
        instance.setNotFoundConfigClass();
      } catch (Exception e) {
        throw new ConfigurationException("Configuration error!", e);

      }
      return instance;
    }

  }
}
