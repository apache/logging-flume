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
package org.apache.flume.agent.embedded;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.flume.FlumeException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TestEmbeddedAgentState {
  private static final String HOSTNAME = "localhost";
  private EmbeddedAgent agent;
  private Map<String, String> properties;

  @Before
  public void setUp() throws Exception {
    agent = new EmbeddedAgent("dummy");
    properties = Maps.newHashMap();
    properties.put("source.type", EmbeddedAgentConfiguration.SOURCE_TYPE_EMBEDDED);
    properties.put("channel.type", "memory");
    properties.put("sinks", "sink1 sink2");
    properties.put("sink1.type", "avro");
    properties.put("sink2.type", "avro");
    properties.put("sink1.hostname", HOSTNAME);
    properties.put("sink1.port", "0");
    properties.put("sink2.hostname", HOSTNAME);
    properties.put("sink2.port", "0");
    properties.put("processor.type", "load_balance");
  }

  @Test(expected = FlumeException.class)
  public void testConfigureWithBadSourceType() {
    properties.put(EmbeddedAgentConfiguration.SOURCE_TYPE, "bad");
    agent.configure(properties);
  }

  @Test(expected = IllegalStateException.class)
  public void testConfigureWhileStarted() {
    try {
      agent.configure(properties);
      agent.start();
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    agent.configure(properties);
  }

  @Test
  public void testConfigureMultipleTimes() {
    agent.configure(properties);
    agent.configure(properties);
  }

  @Test(expected = IllegalStateException.class)
  public void testStartWhileStarted() {
    try {
      agent.configure(properties);
      agent.start();
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    agent.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testStartUnconfigured() {
    agent.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testStopBeforeConfigure() {
    agent.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testStoppedWhileStopped() {
    try {
      agent.configure(properties);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    agent.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testStopAfterStop() {
    try {
      agent.configure(properties);
      agent.start();
      agent.stop();
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    agent.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testStopAfterConfigure() {
    try {
      agent.configure(properties);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    agent.stop();
  }
}
