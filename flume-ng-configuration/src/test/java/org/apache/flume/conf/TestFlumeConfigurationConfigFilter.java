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

import org.apache.flume.Context;
import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestFlumeConfigurationConfigFilter {

  @Test
  public void testFlumeConfigFilterWorks() {
    Properties properties = new Properties();
    properties.put("agent1.channels", "ch0");
    properties.put("agent1.channels.ch0.type", "file");
    properties.put("agent1.channels.ch0.param1", "${f1['param']}");
    properties.put("agent1.channels.ch0.param2", "${f1['param\"]}");
    properties.put("agent1.channels.ch0.param3", "${f1['null']}");
    properties.put("agent1.channels.ch0.param4", "${f1['throw']}");

    properties.put("agent1.sources", "src0");
    properties.put("agent1.sources.src0.type", "multiport_syslogtcp");
    properties.put("agent1.sources.src0.channels", "ch0");
    properties.put("agent1.sources.src0.host", "${f1[host]}");
    properties.put("agent1.sources.src0.ports", "10001 10002 10003");
    properties.put("agent1.sources.src0.portHeader", "${f2[\"port\"]}-${f1['header']}");

    properties.put("agent1.sinks", "sink0");
    properties.put("agent1.sinks.sink0.type", "thrift");
    properties.put("agent1.sinks.sink0.param", "${f2['param']}");
    properties.put("agent1.sinks.sink0.channel", "ch0");

    properties.put("agent1.configfilters", "f1 f2");
    properties.put("agent1.configfilters.f1.type",
        "org.apache.flume.conf.configfilter.MockConfigFilter");
    properties.put("agent1.configfilters.f2.type",
        "org.apache.flume.conf.configfilter.MockConfigFilter");

    FlumeConfiguration conf = new FlumeConfiguration(properties);
    AgentConfiguration agentConfiguration = conf.getConfigurationFor("agent1");
    Context src0 = agentConfiguration.getSourceContext().get("src0");
    assertEquals("filtered_host", src0.getString("host"));
    assertEquals("filtered_port-filtered_header", src0.getString("portHeader"));

    Context sink0 = agentConfiguration.getSinkContext().get("sink0");
    assertEquals("filtered_param", sink0.getString("param"));

    Context ch0 = agentConfiguration.getChannelContext().get("ch0");
    assertEquals("filtered_param", ch0.getString("param1"));
    assertEquals("${f1['param\"]}", ch0.getString("param2"));
    assertEquals("${f1['null']}", ch0.getString("param3"));
    assertEquals("${f1['throw']}", ch0.getString("param4"));
  }

}
