/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master.flow;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.flows.FlowConfigManager;

/**
 * This tests the flow config manager through some simple canned situations and
 */
public class TestFlowConfigManager {

  /**
   * Create simple non translating FlowConfigManager
   */
  FlowConfigManager createSimple() {
    return new FlowConfigManager(new ConfigManager()) {
      @Override
      public ConfigurationManager createConfigMan() {
        return new ConfigManager();
      }
    };
  }

  @Test
  public void testFlowIsolation() throws IOException, FlumeSpecException {
    FlowConfigManager fcm = createSimple();

    fcm.setConfig("node1", "flow1", "null", "null");
    fcm.setConfig("node21", "flow2", "null", "null");
    fcm.setConfig("node22", "flow2", "null", "null");
    fcm.setConfig("node31", "flow3", "null", "null");
    fcm.setConfig("node32", "flow3", "null", "null");
    fcm.setConfig("node33", "flow3", "null", "null");

    assertEquals(1, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(2, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(3, fcm.getConfigManForFlow("flow3").getAllConfigs().size());
    assertEquals(6, fcm.getAllConfigs().size());
    assertEquals(6, fcm.getTranslatedConfigs().size());

    assertEquals("flow1", fcm.getFlowId("node1"));
    assertEquals("flow2", fcm.getFlowId("node21"));
    assertEquals("flow2", fcm.getFlowId("node22"));
    assertEquals("flow3", fcm.getFlowId("node31"));
    assertEquals("flow3", fcm.getFlowId("node32"));
    assertEquals("flow3", fcm.getFlowId("node33"));
  }

  // bulk set
  @SuppressWarnings("serial")
  @Test
  public void testBulkSet() throws IOException {
    FlowConfigManager fcm = createSimple();

    Map<String, FlumeConfigData> configs = new HashMap<String, FlumeConfigData>() {
      {
        put("node1", new FlumeConfigData(1, "null", "null", 1, 1, "flow1"));
        put("node21", new FlumeConfigData(1, "null", "null", 1, 1, "flow2"));
        put("node22", new FlumeConfigData(1, "null", "null", 1, 1, "flow2"));
        put("node31", new FlumeConfigData(1, "null", "null", 1, 1, "flow3"));
        put("node32", new FlumeConfigData(1, "null", "null", 1, 1, "flow3"));
        put("node33", new FlumeConfigData(1, "null", "null", 1, 1, "flow3"));
      }
    };

    fcm.setBulkConfig(configs);

    assertEquals(1, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(2, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(3, fcm.getConfigManForFlow("flow3").getAllConfigs().size());
    assertEquals(6, fcm.getAllConfigs().size());
    assertEquals(6, fcm.getTranslatedConfigs().size());

    assertEquals("flow1", fcm.getFlowId("node1"));
    assertEquals("flow2", fcm.getFlowId("node21"));
    assertEquals("flow2", fcm.getFlowId("node22"));
    assertEquals("flow3", fcm.getFlowId("node31"));
    assertEquals("flow3", fcm.getFlowId("node32"));
    assertEquals("flow3", fcm.getFlowId("node33"));
  }

  @Test
  public void testFlowMove() throws IOException, FlumeSpecException {
    FlowConfigManager fcm = createSimple();

    fcm.setConfig("node1", "flow1", "null", "null");
    fcm.setConfig("node2", "flow2", "null", "null");

    assertEquals(1, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(1, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(2, fcm.getAllConfigs().size());
    assertEquals(2, fcm.getTranslatedConfigs().size());

    // move a node from one flow to another.
    fcm.setConfig("node1", "flow2", "null", "null");
    assertEquals(0, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(2, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(2, fcm.getAllConfigs().size());
    assertEquals(2, fcm.getTranslatedConfigs().size());
  }

  @Test
  public void testFlowRemove() throws IOException, FlumeSpecException {
    FlowConfigManager fcm = createSimple();

    fcm.setConfig("node1", "flow1", "null", "null");
    fcm.setConfig("node2", "flow2", "null", "null");

    assertEquals(1, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(1, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(2, fcm.getAllConfigs().size());
    assertEquals(2, fcm.getTranslatedConfigs().size());

    // move a node from one flow to another.
    fcm.removeLogicalNode("node1");
    assertEquals(0, fcm.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(1, fcm.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(1, fcm.getAllConfigs().size());
    assertEquals(1, fcm.getTranslatedConfigs().size());
  }
}
