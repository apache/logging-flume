/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.sink.SinkGroupConfiguration;

/**
 * <p>Configuration concept for handling multiple sinks working together.</p>
 * @see org.apache.flume.conf.properties.PropertiesFileConfigurationProvider
 */
public class SinkGroup implements Configurable, ConfigurableComponent {
  List<Sink> sinks;
  SinkProcessor processor;
  SinkGroupConfiguration conf;

  public SinkGroup(List<Sink> groupSinks) {
    sinks = groupSinks;
  }

  @Override
  public void configure(Context context) {
    conf = new SinkGroupConfiguration("sinkgrp");
    try {
      conf.configure(context);
    } catch (ConfigurationException e) {
      throw new FlumeException("Invalid Configuration!", e);
    }
    configure(conf);

  }

  public SinkProcessor getProcessor() {
    return processor;
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    this.conf = (SinkGroupConfiguration) conf;
    processor =
        SinkProcessorFactory.getProcessor(this.conf.getProcessorContext(),
            sinks);
  }
}
