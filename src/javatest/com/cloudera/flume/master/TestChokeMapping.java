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

package com.cloudera.flume.master;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;

import com.cloudera.flume.agent.MasterRPC;

import com.cloudera.flume.conf.FlumeConfiguration;

public class TestChokeMapping {
  final public static Logger LOG = Logger.getLogger(TestChokeMapping.class);
  FlumeMaster master = null;

  FlumeConfiguration cfg;

  @Before
  public void setCfg() throws IOException {
    // Isolate tests by only using simple cfg store
    cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_STORE, "memory");
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
  }

  @After
  public void shutdownMaster() {
    if (master != null) {
      master.shutdown();
      master = null;
    }
  }

  /**
   * Checks to make sure that if we add a ChokeID through the master then it
   * gets to the ChokeManager at the FlumeNode.
   */

  @Test
  public void testMasterChokeAddedAtFlumeNode() throws IOException {

    FlumeMaster master = new FlumeMaster(cfg);

    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    master.getSpecMan().addChokeLimit(node.getPhysicalNodeName(), "bar", 786);

    // This method updateChokeLimitMap is the same method call in the
    // checkLogicalNodes() in the LivenessManager
    node.getChokeManager().updateChokeLimitMap(
        (HashMap<String, Integer>) master.getSpecMan().getChokeMap(
            node.getPhysicalNodeName()));

    if (!node.getChokeManager().isChokeId("bar")) {
      LOG.error("TestChokeMapping Failed!!");
      LOG
          .error("The ChokeID 'bar' did not get registered with the ChokeManager!!");
      fail("TestChokeMapping Failed!!");
    }
    LOG.info("TestChokeMapping successful!!");
  }

}
