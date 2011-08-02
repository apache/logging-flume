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

package com.cloudera.flume.agent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.thrift.FlumeConfigData;

public class LogicalNodeTest {

  /**
   * Test that checkConfig has the correct versioning behaviour
   */
  @Test
  public void testCheckConfig() {
    LogicalNode node = new LogicalNode(new Context(), "test-logical-node");

    assertFalse(node.checkConfig(null));

    // Are new configs accepted?
    FlumeConfigData cfgData = new FlumeConfigData(0, "null", "null", 0, 0,
        "my-test-flow");
    assertTrue(node.checkConfig(cfgData));
    assertFalse(node.checkConfig(cfgData));

    // Are updated configs accepted?
    FlumeConfigData cfgData2 = new FlumeConfigData(0, "null", "null", 1, 0,
        "my-test-flow");
    assertTrue(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData));

    // Are configs with the same version rejected?
    FlumeConfigData cfgData3 = new FlumeConfigData(0, "null", "null", 1, 1,
        "my-test-flow");
    assertFalse(node.checkConfig(cfgData));
    assertFalse(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData3));

  }

}
