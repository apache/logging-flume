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
package org.apache.flume.conf.sink;

import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType;
import org.apache.flume.conf.source.SourceConfiguration;

public class SinkProcessorConfiguration extends ComponentConfiguration {
  protected Set<String> sinks;

  protected SinkProcessorConfiguration(String componentName) {
    super(componentName);
    setType("default");
  }

  public void configure(Context context) throws ConfigurationException {

  }

  public Set<String> getSinks() {
    return sinks;
  }

  public void setSinks(Set<String> sinks) {
    this.sinks = sinks;
  }

  public enum SinkProcessorConfigurationType {
    /**
     * Load balacing channel selector
     */
    LOAD_BALANCE("org.apache.flume.conf.sink.LoadBalancingSinkProcessorConfiguration"),
    /**
     * Failover processor
     *
     * @see FailoverSinkProcessor
     */
    FAILOVER("org.apache.flume.conf.sink.FailoverSinkProcessorConfiguration"),

    /**
     * Standard processor
     *
     * @see DefaultSinkProcessor
     */
    DEFAULT(null);
    private final String processorClassName;

    private SinkProcessorConfigurationType(String processorClassName) {
      this.processorClassName = processorClassName;
    }

    public String getSinkProcessorConfigurationType() {
      return processorClassName;
    }

    @SuppressWarnings("unchecked")
    public SinkProcessorConfiguration getConfiguration(String name)
        throws ConfigurationException {
      Class<? extends SinkProcessorConfiguration> clazz;
      SinkProcessorConfiguration instance = null;
      try {
        if (processorClassName != null) {
          clazz =
              (Class<? extends SinkProcessorConfiguration>) Class
                  .forName(processorClassName);
          instance = clazz.getConstructor(String.class).newInstance(name);

        } else {
          return new SinkProcessorConfiguration(name);
        }
      } catch (ClassNotFoundException e) {
        // Could not find the configuration stub, do basic validation
        instance = new SinkProcessorConfiguration(name);
        // Let the caller know that this was created because of this exception.
        instance.setNotFoundConfigClass();
      } catch (Exception e) {
        throw new ConfigurationException(
            "Could not instantiate configuration!", e);
      }
      return instance;
    }
  }

}
