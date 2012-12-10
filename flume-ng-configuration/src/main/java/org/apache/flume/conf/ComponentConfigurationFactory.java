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
package org.apache.flume.conf;

import org.apache.flume.conf.ComponentConfiguration.ComponentType;
import org.apache.flume.conf.channel.ChannelConfiguration.ChannelConfigurationType;
import org.apache.flume.conf.channel.ChannelSelectorConfiguration.ChannelSelectorConfigurationType;
import org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType;
import org.apache.flume.conf.sink.SinkGroupConfiguration;
import org.apache.flume.conf.sink.SinkProcessorConfiguration.SinkProcessorConfigurationType;
import org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType;

public class ComponentConfigurationFactory {
  @SuppressWarnings("unchecked")
  public static ComponentConfiguration
  create(String name, String type, ComponentType component)
      throws ConfigurationException {
    Class<? extends ComponentConfiguration> confType = null;

    if (type == null) {
      throw new ConfigurationException(
          "Cannot create component without knowing its type!");
    }
    try {
      confType = (Class<? extends ComponentConfiguration>) Class.forName(type);
      return confType.getConstructor(String.class).newInstance(type);
    } catch (Exception ignored) {
      try {
        type = type.toUpperCase();
        switch(component){
          case SOURCE:
            return SourceConfigurationType.valueOf(type.toUpperCase())
                .getConfiguration(name);
          case SINK:
            return SinkConfigurationType.valueOf(type.toUpperCase())
                .getConfiguration(name);
          case CHANNEL:
            return ChannelConfigurationType.valueOf(type.toUpperCase())
                .getConfiguration(name);
          case SINK_PROCESSOR:
            return SinkProcessorConfigurationType.valueOf(type.toUpperCase())
                .getConfiguration(name);
          case CHANNELSELECTOR:
            return ChannelSelectorConfigurationType.valueOf(type.toUpperCase())
                .getConfiguration(name);
          case SINKGROUP:
            return new SinkGroupConfiguration(name);
          default:
            throw new ConfigurationException(
                "Cannot create configuration. Unknown Type specified: " +
                    type);
        }
      } catch (ConfigurationException e) {
        throw e;
      } catch (Exception e) {
        throw new ConfigurationException("Could not create configuration! " +
            " Due to " + e.getClass().getSimpleName() + ": " + e.getMessage(),
            e);
      }
    }
  }
}
