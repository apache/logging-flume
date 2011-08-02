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
package com.cloudera.flume.conf;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestFlumeConfigData {
  
  @Test
  public void testFlumeConfigConstructors() {
    // Standard constrcuter
    FlumeConfigData config = new FlumeConfigData((long) 123, "mySourceConfig", 
        "mySinkConfig", (long) 456, (long) 789, "myFlowID");
    assureValidFlumeConfigData(config);
  
    // Copy constructor
    FlumeConfigData config2 = new FlumeConfigData(config);
    assureValidFlumeConfigData(config);
    assureValidFlumeConfigData(config2);
    
    // Empty constructor
    FlumeConfigData config3 = new FlumeConfigData();
    config3.timestamp = (long) 123;
    config3.sourceConfig = "mySourceConfig";
    config3.sinkConfig = "mySinkConfig";
    config3.sourceVersion = (long) 456;
    config3.sinkVersion = (long) 789;
    config3.flowID = "myFlowID";
    assureValidFlumeConfigData(config3);
  }
  
  public void assureValidFlumeConfigData(FlumeConfigData config) {
    assertEquals(config.timestamp, (long) 123);
    assertEquals(config.getTimestamp(), (long) 123);
    
    assertEquals(config.sourceConfig, "mySourceConfig");
    assertEquals(config.getSourceConfig(), "mySourceConfig");
    
    assertEquals(config.sinkConfig, "mySinkConfig");
    assertEquals(config.getSinkConfig(), "mySinkConfig");
    
    assertEquals(config.sourceVersion, (long) 456);
    assertEquals(config.getSourceVersion(), (long) 456);
    
    assertEquals(config.sinkVersion, (long) 789);
    assertEquals(config.getSinkVersion(), (long) 789);
    
    assertEquals(config.flowID, "myFlowID");
    assertEquals(config.getFlowID(), "myFlowID");
  }
}
