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
package org.apache.flume.conf.sink;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ComponentConfigurationFactory;
import org.apache.flume.conf.ConfigurationException;

public class SinkGroupConfiguration extends ComponentConfiguration {
  private Context processorContext;
  private List<String> sinks;
  private SinkProcessorConfiguration processorConf;

  public SinkGroupConfiguration(String name) {
    super(name);
    setType(ComponentType.SINKGROUP.getComponentType());
  }

  public void setSinks(List<String> sinks) {
    this.sinks = sinks;
  }

  public List<String> getSinks() {
    return sinks;
  }

  @Override
  public void configure(Context context) throws ConfigurationException {
    super.configure(context);
    sinks = Arrays.asList(context.getString(
        BasicConfigurationConstants.CONFIG_SINKS).split("\\s+"));
    Map<String, String> params = context.getSubProperties(
        BasicConfigurationConstants.CONFIG_SINK_PROCESSOR_PREFIX);
    processorContext = new Context();
    processorContext.putAll(params);
    SinkProcessorType spType = getKnownSinkProcessor(processorContext.getString(
            BasicConfigurationConstants.CONFIG_TYPE));

    if (spType != null) {
      processorConf =
          (SinkProcessorConfiguration) ComponentConfigurationFactory.create(
              this.getComponentName() + "-processor",
              spType.toString(),
              ComponentType.SINK_PROCESSOR);
      if (processorConf != null) {
        processorConf.setSinks(new HashSet<String>(sinks));
        processorConf.configure(processorContext);
      }
    }
    setConfigured();
  }

  public Context getProcessorContext() {
    return processorContext;
  }

  public void setProcessorContext(Context processorContext) {
    this.processorContext = processorContext;
  }

  public SinkProcessorConfiguration getSinkProcessorConfiguration() {
    return processorConf;
  }

  public void setSinkProcessorConfiguration(SinkProcessorConfiguration conf) {
    this.processorConf = conf;
  }

  private SinkProcessorType getKnownSinkProcessor(String type) {
    SinkProcessorType[] values = SinkProcessorType.values();
    for (SinkProcessorType value : values) {
      if (value.toString().equalsIgnoreCase(type)) return value;
      String sinkProcessClassName = value.getSinkProcessorClassName();
      if (sinkProcessClassName != null
          && sinkProcessClassName.equalsIgnoreCase(type)){
        return value;
      }
    }
    return null;
  }
}
