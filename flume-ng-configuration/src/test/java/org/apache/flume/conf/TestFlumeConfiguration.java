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

import java.util.Properties;

import junit.framework.Assert;

import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.junit.Test;

public class TestFlumeConfiguration {

  /**
   * Test fails without FLUME-1743
   */
  @Test
  public void testFLUME1743() throws Exception {
    Properties properties = new Properties();
    properties.put("agent1.channels", "ch0");
    properties.put("agent1.channels.ch0.type", "memory");

    properties.put("agent1.sources", "src0");
    properties.put("agent1.sources.src0.type", "multiport_syslogtcp");
    properties.put("agent1.sources.src0.channels", "ch0");
    properties.put("agent1.sources.src0.host", "localhost");
    properties.put("agent1.sources.src0.ports", "10001 10002 10003");
    properties.put("agent1.sources.src0.portHeader", "port");

    properties.put("agent1.sinks", "sink0");
    properties.put("agent1.sinks.sink0.type", "null");
    properties.put("agent1.sinks.sink0.channel", "ch0");

    FlumeConfiguration conf = new FlumeConfiguration(properties);
    AgentConfiguration agentConfiguration = conf.getConfigurationFor("agent1");
    Assert.assertEquals(String.valueOf(agentConfiguration.getSourceSet()), 1,
        agentConfiguration.getSourceSet().size());
    Assert.assertEquals(String.valueOf(agentConfiguration.getChannelSet()), 1,
        agentConfiguration.getChannelSet().size());
    Assert.assertEquals(String.valueOf(agentConfiguration.getSinkSet()), 1,
        agentConfiguration.getSinkSet().size());
    Assert.assertTrue(agentConfiguration.getSourceSet().contains("src0"));
    Assert.assertTrue(agentConfiguration.getChannelSet().contains("ch0"));
    Assert.assertTrue(agentConfiguration.getSinkSet().contains("sink0"));
  }
}
