/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.configfilter;

import org.apache.flume.conf.FlumeConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EnvironmentVariableConfigFilterTest {

  public static final String AGENT = "agent";
  public static final String SINKS = AGENT + ".sinks";
  public static final String SOURCES = AGENT + ".sources";
  public static final String CHANNELS = AGENT + ".channels";

  @Rule
  public final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  @Test
  public void filter() {
    environmentVariables.set("my_password_key", "filtered");
    environmentVariables.set("my_password_key2", "filtered2");

    Map<String, String> properties = new HashMap();
    properties.put(SOURCES, "s1");
    properties.put(SOURCES + ".s1.type", "s1_type");
    properties.put(SOURCES + ".s1.channels", "c1");
    properties.put(SOURCES + ".s1.password", "${f1['my_password_key']}");
    properties.put(CHANNELS, "c1");
    properties.put(CHANNELS + ".c1.type", "c1_type");
    properties.put(CHANNELS + ".c1.secret", "${f1['my_password_key']}");
    properties.put(SINKS, "k1");
    properties.put(SINKS + ".k1.type", "k1_type");
    properties.put(SINKS + ".k1.channel", "c1");
    properties.put(SINKS + ".k1.token", "${f1['my_password_key2']}");
    properties.put(AGENT + ".configfilters", "f1");
    properties.put(AGENT + ".configfilters.f1.type", "env");

    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    String actual;
    String expected = "filtered";

    actual = flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
        .getString("password");
    assertEquals(expected, actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getChannelContext().get("c1")
        .getString("secret");
    assertEquals(expected, actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getSinkContext().get("k1")
        .getString("token");
    expected = "filtered2";
    assertEquals(expected, actual);

  }
}