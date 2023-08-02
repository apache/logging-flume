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

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.ERROR;
import static org.apache.flume.conf.FlumeConfigurationErrorType.*;
import static org.junit.Assert.assertEquals;

public class TestFlumeConfiguration {

  /**
   * Test fails without FLUME-1743
   */
  @Test
  public void testFLUME1743() {
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

    properties.put("agent1.configfilters", "f1");
    properties.put("agent1.configfilters.f1.type", "env");

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
    Assert.assertTrue(agentConfiguration.getConfigFilterSet().contains("f1"));
  }

  @Test
  public void testFlumeConfigAdsErrorOnNullName() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(null, "something");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), AGENT_NAME_MISSING, "", "", ERROR);
  }

  @Test
  public void testFlumeConfigAddsErrorOnNullValue() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("something", null);
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), AGENT_NAME_MISSING, "", "", ERROR);
  }


  @Test
  public void testFlumeConfigAddsErrorOnEmptyValue() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("something", "");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), PROPERTY_VALUE_NULL, "something", "", ERROR);
  }


  @Test
  public void testFlumeConfigAddsErrorOnNoAgentNameValue() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("something", "value");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), AGENT_NAME_MISSING, "something", "", ERROR);
  }

  @Test
  public void testFlumeConfigAddsErrorOnEmptyAgentNameValue() {
    Properties properties = new Properties();
    properties.put(".something", "value");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), AGENT_NAME_MISSING, ".something", "", ERROR);
  }

  @Test
  public void testFlumeConfigAddsErrorOnEmptyPropertyName() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("agent.", "something");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(1, configurationErrors.size());
    assertError(configurationErrors.get(0), PROPERTY_NAME_NULL, "agent.", "", ERROR);
  }

  @Test
  public void testFlumeConfigAddsErrorOnInvalidConfig() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("agent.channels", "c1");
    properties.put("agent.channel.c1", "cc1");
    FlumeConfiguration config = new FlumeConfiguration(properties);

    List<FlumeConfigurationError> configurationErrors = config.getConfigurationErrors();
    assertEquals(4, configurationErrors.size());
    assertError(configurationErrors.get(0), INVALID_PROPERTY, "agent", "channel.c1", ERROR);
    assertError(configurationErrors.get(1), CONFIG_ERROR, "agent", "c1", ERROR);
    assertError(configurationErrors.get(2), PROPERTY_VALUE_NULL, "agent", "channels", ERROR);
    assertError(configurationErrors.get(3), AGENT_CONFIGURATION_INVALID, "agent", "", ERROR);
  }

  private void assertError(
          FlumeConfigurationError error,
          FlumeConfigurationErrorType agentNameMissing,
          String componentName, String key,
          ErrorOrWarning eow
  ) {
    assertEquals(agentNameMissing, error.getErrorType());
    assertEquals("ComponentName mismatch.", componentName, error.getComponentName());
    assertEquals("Key mismatch.", key, error.getKey());
    assertEquals(eow, error.getErrorOrWarning());
  }

}
