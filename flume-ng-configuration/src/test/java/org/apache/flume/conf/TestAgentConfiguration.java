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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.ERROR;
import static org.junit.Assert.*;

public class TestAgentConfiguration {

  public static final Map<String, String> PROPERTIES = new HashMap<>();
  public static final String AGENT = "agent";
  public static final String SINKS = AGENT + ".sinks";
  public static final String SOURCES = AGENT + ".sources";
  public static final String CHANNELS = AGENT + ".channels";

  @BeforeClass
  public static void setupClass() {
    PROPERTIES.put(SOURCES, "s1 s2");
    PROPERTIES.put(SOURCES + ".s1.type", "s1_type");
    PROPERTIES.put(SOURCES + ".s1.channels", "c1");
    PROPERTIES.put(SOURCES + ".s2.type", "jms");
    PROPERTIES.put(SOURCES + ".s2.channels", "c2");
    PROPERTIES.put(CHANNELS, "c1 c2");
    PROPERTIES.put(CHANNELS + ".c1.type", "c1_type");
    PROPERTIES.put(CHANNELS + ".c2.type", "memory");
    PROPERTIES.put(SINKS, "k1 k2");
    PROPERTIES.put(SINKS + ".k1.type", "k1_type");
    PROPERTIES.put(SINKS + ".k2.type", "null");
    PROPERTIES.put(SINKS + ".k1.channel", "c1");
    PROPERTIES.put(SINKS + ".k2.channel", "c2");
    PROPERTIES.put(AGENT + ".sinkgroups", "g1");
    PROPERTIES.put(AGENT + ".sinkgroups.g1.sinks", "k1 k2");
    PROPERTIES.put(AGENT + ".configfilters", "f1 f2");
    PROPERTIES.put(AGENT + ".configfilters.f1.type", "f1_type");
    PROPERTIES.put(AGENT + ".configfilters.f2.type", "env");
  }

  @Test
  public void testConfigHasNoErrors() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    assertTrue(configuration.getConfigurationErrors().isEmpty());
  }

  @Test
  public void testSourcesAdded() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Set<String> sourceSet = configuration.getConfigurationFor(AGENT).getSourceSet();
    assertEquals(new HashSet<>(Arrays.asList("s1", "s2")), sourceSet);
  }

  @Test
  public void testFiltersAdded() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Set<String> configFilterSet = configuration.getConfigurationFor(AGENT).getConfigFilterSet();
    assertEquals(new HashSet<>(Arrays.asList("f1", "f2")), configFilterSet);
  }

  @Test
  public void testSinksAdded() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Set<String> sinkSet = configuration.getConfigurationFor(AGENT).getSinkSet();
    assertEquals(new HashSet<>(Arrays.asList("k1", "k2")), sinkSet);
  }

  @Test
  public void testChannelsAdded() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Set<String> channelSet = configuration.getConfigurationFor(AGENT).getChannelSet();
    assertEquals(new HashSet<>(Arrays.asList("c1", "c2")), channelSet);
  }

  @Test
  public void testSinkGroupsAdded() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Set<String> sinkSet = configuration.getConfigurationFor(AGENT).getSinkgroupSet();
    assertEquals(new HashSet<>(Arrays.asList("g1")), sinkSet);
  }

  @Test
  public void testConfigFiltersMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, Context> contextMap =
        configuration.getConfigurationFor(AGENT).getConfigFilterContext();
    assertEquals("f1_type", contextMap.get("f1").getString("type"));
  }

  @Test
  public void testSourcesMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, Context> contextMap = configuration.getConfigurationFor(AGENT).getSourceContext();
    assertEquals("s1_type", contextMap.get("s1").getString("type"));
  }

  @Test
  public void testSinksMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, Context> contextMap = configuration.getConfigurationFor(AGENT).getSinkContext();
    assertEquals("k1_type", contextMap.get("k1").getString("type"));
  }

  @Test
  public void testChannelsMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, Context> contextMap = configuration.getConfigurationFor(AGENT).getChannelContext();
    assertEquals("c1_type", contextMap.get("c1").getString("type"));
  }

  @Test
  public void testChannelsConfigMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, ComponentConfiguration> configMap =
        configuration.getConfigurationFor(AGENT).getChannelConfigMap();
    assertEquals("memory", configMap.get("c2").getType());
  }

  @Test
  public void testConfigFilterConfigMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, ComponentConfiguration> configMap =
        configuration.getConfigurationFor(AGENT).getConfigFilterConfigMap();
    assertEquals("env", configMap.get("f2").getType());
  }

  @Test
  public void testSourceConfigMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, ComponentConfiguration> configMap =
        configuration.getConfigurationFor(AGENT).getSourceConfigMap();
    assertEquals("jms", configMap.get("s2").getType());
  }

  @Test
  public void testSinkConfigMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, ComponentConfiguration> configMap =
        configuration.getConfigurationFor(AGENT).getSinkConfigMap();
    assertEquals("null", configMap.get("k2").getType());
  }

  @Test
  public void testSinkgroupConfigMappedCorrectly() {
    FlumeConfiguration configuration = new FlumeConfiguration(PROPERTIES);
    Map<String, ComponentConfiguration> configMap =
        configuration.getConfigurationFor(AGENT).getSinkGroupConfigMap();
    assertEquals("Sinkgroup", configMap.get("g1").getType());
  }

  @Test
  public void testNoChannelIsInvalid() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CHANNELS, "");
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);

    assertFalse(flumeConfiguration.getConfigurationErrors().isEmpty());
    assertNull(flumeConfiguration.getConfigurationFor(AGENT));
  }

  @Test
  public void testNoSourcesIsValid() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.remove(SOURCES);
    properties.remove(SOURCES + ".s1.type");
    properties.remove(SOURCES + ".s1.channels");
    properties.remove(SOURCES + ".s2.type");
    properties.remove(SOURCES + ".s2.channels");
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);

    assertConfigHasNoError(flumeConfiguration);
    assertNotNull(flumeConfiguration.getConfigurationFor(AGENT));
  }

  @Test
  public void testNoSinksIsValid() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.remove(SINKS);
    properties.remove(SINKS + ".k1.type", "k1_type");
    properties.remove(SINKS + ".k2.type", "null");
    properties.remove(SINKS + ".k1.channel", "c1");
    properties.remove(SINKS + ".k2.channel", "c2");
    properties.remove(AGENT + ".sinkgroups", "g1");
    properties.remove(AGENT + ".sinkgroups.g1.sinks", "k1 k2");

    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);

    assertConfigHasNoError(flumeConfiguration);
    assertNotNull(flumeConfiguration.getConfigurationFor(AGENT));
  }

  private void assertConfigHasNoError(FlumeConfiguration configuration) {
    List<FlumeConfigurationError> configurationErrors = configuration.getConfigurationErrors();
    long count = 0L;
    for (FlumeConfigurationError e : configurationErrors) {
      if (e.getErrorOrWarning() == ERROR) {
        count++;
      }
    }
    assertTrue(count == 0);
  }

  @Test
  public void testNoSourcesAndNoSinksIsInvalid() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(SOURCES, "");
    properties.put(SINKS, "");
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);

    assertFalse(flumeConfiguration.getConfigurationErrors().isEmpty());
    assertNull(flumeConfiguration.getConfigurationFor(AGENT));
  }
}
