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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LivenessManager;
import com.cloudera.flume.agent.MasterRPC;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.util.NetUtils;
import com.google.common.collect.Multimap;

public class TestLogicalNodeMapping {

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
   * Checks to make sure that nodes specified at the master get spawned at the
   * node.
   * 
   * Even though only two nodes are set, there should be three because we
   * currently have an invariant where each physical node must have at least on
   * logical node, which is named the same name as the physical node name.
   * 
   * This may go away when we "bulkify" the node-master comms into one rpc call.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testMasterLogicalNodeCheckAutoLogicalNode() throws IOException,
      InterruptedException {
    FlumeMaster master = new FlumeMaster(cfg);
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "bar");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "baz");

    MasterRPC rpc = new DirectMasterRPC(master);

    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();
    // the two added nodes, plus the always present physnode/logical
    assertEquals(3, node.getLogicalNodeManager().getNodes().size());

  }

  /**
   * Checks to make sure that nodes specified at the master get spawned at the
   * node.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testMasterLogicalNodeSpawnedAtNode() throws IOException,
      InterruptedException {
    FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    master.getSpecMan().addLogicalNode(NetUtils.localhost(),
        node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "bar");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "baz");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();
    // the two added nodes, plus the always present physnode/logical
    assertEquals(3, node.getLogicalNodeManager().getNodes().size());
  }

  /**
   * Checks to make sure that an unmapped node correctly transitions to
   * DECOMMISSIONED
   */
  @Test
  public void testUnmappedLogicalNodeGetsDecommissioned() throws IOException,
      InterruptedException {
    FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    master.getSpecMan().addLogicalNode(node.getPhysicalNodeName(), "foo");

    master.getStatMan().updateHeartbeatStatus(NetUtils.localhost(),
        node.getPhysicalNodeName(), "foo", NodeState.ACTIVE, 10);

    master.getSpecMan().unmapLogicalNode(NetUtils.localhost(), "foo");

    master.getStatMan().checkup();

    assertEquals(NodeState.DECOMMISSIONED, master.getStatMan()
        .getNodeStatuses().get("foo").state);

    master.getSpecMan().addLogicalNode(node.getPhysicalNodeName(), "foo");
    master.getStatMan().updateHeartbeatStatus(NetUtils.localhost(),
        node.getPhysicalNodeName(), "foo", NodeState.ACTIVE, 10);

    master.getStatMan().checkup();

    assertEquals(NodeState.ACTIVE, master.getStatMan().getNodeStatuses().get(
        "foo").state);
  }

  /**
   * Checks to make sure that nodes specified at the master get spawned at the
   * node.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testMasterDecomission() throws IOException, InterruptedException {
    FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    master.getSpecMan().addLogicalNode(NetUtils.localhost(),
        node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "bar");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "baz");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();
    // the two added nodes, plus the always present physnode/logical
    assertEquals(3, node.getLogicalNodeManager().getNodes().size());
  }

  @Test
  public void testUnmapLogicalNode() throws IOException, InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    FlumeMaster master = new FlumeMaster(new CommandManager(),
        new ConfigManager(), new StatusManager(), new MasterAckManager(), cfg);
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    String local = NetUtils.localhost();

    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();

    assertEquals(local, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, "bar");
    liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, "baz");
    liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, local);
    liveMan.checkLogicalNodes();
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
  }

  @Test
  public void testDuplicateSpawn() throws IOException, InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    FlumeMaster master = new FlumeMaster(new CommandManager(),
        new ConfigManager(), new StatusManager(), new MasterAckManager(), cfg);
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    String local = NetUtils.localhost();

    // these are spawn commands
    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");

    // there should not be duplicates in the mapping table.
    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");

    Multimap<String, String> mapping = master.getSpecMan().getLogicalNodeMap();
    assertEquals(3, mapping.size());

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();
    assertEquals(3, node.getLogicalNodeManager().getNodes().size());
  }

  @Test
  public void testRemoveLogicalNode() throws IOException, FlumeSpecException,
      InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    FlumeMaster master = new FlumeMaster(new CommandManager(),
        new ConfigManager(), new StatusManager(), new MasterAckManager(), cfg);

    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    String local = NetUtils.localhost();

    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");
    master.getSpecMan().setConfig(local, "my-test-flow", "null", "null");
    master.getSpecMan().setConfig("bar", "my-test-flow", "null", "null");
    master.getSpecMan().setConfig("baz", "my-test-flow", "null", "null");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();

    assertEquals(local, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNotNull(master.getSpecMan().getConfig("bar"));
    assertNotNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode("bar");
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNotNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode("baz");
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode(local);
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
  }

  /**
   * Checks to make sure that nodes specified at the master get spawned at the
   * node.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testZKMasterDecomission() throws IOException,
      TTransportException, InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    cfg.set(FlumeConfiguration.MASTER_STORE, "zookeeper");
    master = new FlumeMaster(new CommandManager(), new ConfigManager(),
        new StatusManager(), new MasterAckManager(), cfg);
    master.serve();
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    master.getSpecMan().addLogicalNode(NetUtils.localhost(),
        node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "bar");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), "baz");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();
    // the two added nodes, plus the always present physnode/logical
    assertEquals(3, node.getLogicalNodeManager().getNodes().size());
  }

  @Test
  public void testZKUnmapLogicalNode() throws IOException, TTransportException,
      InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    cfg.set(FlumeConfiguration.MASTER_STORE, "zookeeper");

    master = new FlumeMaster(new CommandManager(), new ConfigManager(),
        new StatusManager(), new MasterAckManager(), cfg);

    master.serve();
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    String local = NetUtils.localhost();

    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.checkLogicalNodes();

    assertEquals(local, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, "bar");
    liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, "baz");
    liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().unmapLogicalNode(local, local);
    liveMan.checkLogicalNodes();
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
  }

  @Test
  public void testZKRemoveLogicalNode() throws IOException, FlumeSpecException,
      TTransportException, InterruptedException {
    // use the simple command manger, non-gossip ackmanager
    cfg.set(FlumeConfiguration.MASTER_STORE, "zookeeper");

    master = new FlumeMaster(new CommandManager(), new ConfigManager(),
        new StatusManager(), new MasterAckManager(), cfg);
    master.serve();
    MasterRPC rpc = new DirectMasterRPC(master);
    FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    String local = NetUtils.localhost();

    master.getSpecMan().addLogicalNode(local, node.getPhysicalNodeName());
    master.getSpecMan().addLogicalNode(local, "bar");
    master.getSpecMan().addLogicalNode(local, "baz");
    master.getSpecMan().setConfig(local, "my-test-flow", "null", "null");
    master.getSpecMan().setConfig("bar", "my-test-flow", "null", "null");
    master.getSpecMan().setConfig("baz", "my-test-flow", "null", "null");

    LivenessManager liveMan = node.getLivenessManager();
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();

    assertEquals(local, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNotNull(master.getSpecMan().getConfig("bar"));
    assertNotNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode("bar");
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(local, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNotNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode("baz");
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNotNull(master.getSpecMan().getConfig(local));

    master.getSpecMan().removeLogicalNode(local);
    liveMan.heartbeatChecks();
    // liveMan.checkLogicalNodes();
    assertNull(master.getSpecMan().getConfig("bar"));
    assertNull(master.getSpecMan().getConfig("baz"));
    assertNull(master.getSpecMan().getConfig(local));
    assertEquals(null, master.getSpecMan().getPhysicalNode("bar"));
    assertEquals(null, master.getSpecMan().getPhysicalNode("baz"));
    assertEquals(local, master.getSpecMan().getPhysicalNode(local));
  }
}
