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
package com.cloudera.flume.agent.diskfailover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LivenessManager;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.agent.MasterRPC;
import com.cloudera.flume.agent.durability.NaiveFileWALDeco;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Driver.DriverState;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;

// TODO on failures, these tests leak data to the flume-${user} dir.
public class TestDiskFailoverAgent {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestDiskFailoverAgent.class);

  FlumeMaster master = null;
  FlumeConfiguration cfg;
  File tmpDir;

  @Before
  public void setCfg() throws IOException {
    LOG.info("============================================================");

    tmpDir = FileUtil.mktempdir();

    // Isolate tests by only using simple cfg store
    cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_STORE, "memory");
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
    cfg.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpDir.getAbsolutePath());
    org.apache.log4j.Logger.getLogger(NaiveFileWALDeco.class).setLevel(
        Level.DEBUG);
  }

  @After
  public void shutdownMaster() throws IOException {
    if (master != null) {
      master.shutdown();
      master = null;
    }
    FileUtil.rmr(tmpDir);
  }

  /**
   * This test starts a DFO agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decomissioned. As part of decomissioning, the act of closing
   * the dfo agent should not hang the node.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Test
  public void testActiveDFOClose() throws InterruptedException, IOException,
      FlumeSpecException {
    final String lnode = "DFOSimple";
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());
    LivenessManager liveMan = node.getLivenessManager();
    // update config node to something that will be interrupted.
    LOG.info("setting to invalid dfo host");
    master.getSpecMan().setConfig(lnode, "flow", "asciisynth(0)",
        "agentDFOSink(\"localhost\", 12345)");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), lnode);
    liveMan.heartbeatChecks();

    LogicalNode n = node.getLogicalNodeManager().get(lnode);
    Driver d = n.getDriver();
    assertTrue("Attempting to start driver timed out",
        d.waitForAtLeastState(DriverState.ACTIVE, 10000));

    // update config node to something that will be interrupted.
    LOG.info("!!! decommissioning node on master");
    master.getSpecMan().removeLogicalNode(lnode);

    // as node do heartbeat and update due to decommission
    liveMan.heartbeatChecks();
    LOG.info("!!! node should be decommissioning on node");
    assertTrue("Attempting to decommission driver timed out",
        d.waitForAtLeastState(DriverState.IDLE, 10000));

    assertEquals("Only expected default logical node", 1, node
        .getLogicalNodeManager().getNodes().size());
    assertNull(node.getLogicalNodeManager().get(lnode));
  }

  /**
   * This test starts a DFO agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decommissioned. As part of decommissioning, the act of closing
   * the dfo agent should not hang the node.
   * 
   * This test differs from the previous by having an bad dns name/request that
   * will eventually fail (ubuntu/java1.6 takes about 10s)
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Test
  public void testActiveDFOCloseBadDNS() throws InterruptedException,
      IOException, FlumeSpecException {
    final String lnode = "DFOBadDNS";
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());
    LivenessManager liveMan = node.getLivenessManager();
    // update config node to something that will be interrupted.
    LOG.info("setting to invalid dfo host");
    master.getSpecMan().setConfig("node2", "flow", "asciisynth(0)",
        "agentDFOSink(\"invalid\", 12346)");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), lnode);
    liveMan.heartbeatChecks();

    LogicalNode n = node.getLogicalNodeManager().get(lnode);
    Driver d = n.getDriver();
    assertTrue("Attempting to start driver timed out",
        d.waitForAtLeastState(DriverState.ACTIVE, 20000));

    // update config node to something that will be interrupted.
    LOG.info("!!! decommissioning node on master");
    master.getSpecMan().removeLogicalNode(lnode);

    liveMan.heartbeatChecks();
    LOG.info("!!! logical node should be decommissioning on node");
    assertTrue("Attempting to start driver timed out",
        d.waitForAtLeastState(DriverState.IDLE, 20000));
    LOG.info("Clean close.");

    // false means timeout, takes about 10 seconds to shutdown.
    assertTrue("Attempting to decommission driver timed out",
        d.waitForAtLeastState(DriverState.IDLE, 10000));

    assertEquals("Only expected default logical node", 1, node
        .getLogicalNodeManager().getNodes().size());
    assertNull(node.getLogicalNodeManager().get(lnode));
  }

  /**
   * This test starts a E2E agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decommissioned. As part of decommissioning, the act of closing
   * the E2E agent should not hang the node.
   */
  @Test
  public void testActiveE2ECloseSimple() throws InterruptedException,
      IOException, FlumeSpecException {
    final String lnode = "e2eSimple";
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    LivenessManager liveMan = node.getLivenessManager();
    // update config node to something that will be interrupted.
    LOG.info("setting to invalid e2e host");
    master.getSpecMan().setConfig(lnode, "flow", "asciisynth(0)",
        "agentE2ESink(\"localhost\", 12347)");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), lnode);
    liveMan.heartbeatChecks();

    // TODO It we only wait for opening state, this test can hang
    LogicalNode n = node.getLogicalNodeManager().get(lnode);
    Driver d = n.getDriver();
    assertTrue("Attempting to start driver timed out",
        d.waitForAtLeastState(DriverState.ACTIVE, 10000));

    // update config node to something that will be interrupted.
    LOG.info("!!! decommissioning node on master");
    master.getSpecMan().removeLogicalNode(lnode);
    liveMan.heartbeatChecks();
    assertTrue("Attempting to stop driver timed out",
        d.waitForAtLeastState(DriverState.ERROR, 15000));
  }

  /**
   * This test starts a E2E agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decommissioned. As part of decommissioning, the act of closing
   * the E2E agent should not hang the node.
   * 
   * This test differs from the previous by having an bad dns name/request that
   * will eventually fail (ubuntu/java1.6 takes about 10s)
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Test
  public void testActiveE2ECloseBadDNS() throws InterruptedException,
      IOException, FlumeSpecException {
    final String lnode = "e2eBadDNS";
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    LivenessManager liveMan = node.getLivenessManager();
    // update config node to something that will be interrupted.
    LOG.info("setting to invalid e2e host");
    master.getSpecMan().setConfig(lnode, "flow", "asciisynth(0)",
        "agentE2ESink(\"localhost\", 12348)");
    master.getSpecMan().addLogicalNode(NetUtils.localhost(), lnode);
    liveMan.heartbeatChecks();

    // TODO It we only wait for opening state, this test can hang
    LogicalNode n = node.getLogicalNodeManager().get(lnode);
    Driver d = n.getDriver();
    assertTrue("Attempting to start driver timed out",
        d.waitForAtLeastState(DriverState.ACTIVE, 15000));

    // update config node to something that will be interrupted.
    LOG.info("!!! decommissioning node on master");
    master.getSpecMan().removeLogicalNode(lnode);
    liveMan.heartbeatChecks();
    assertTrue("Attempting to stop driver timed out",
        d.waitForAtLeastState(DriverState.ERROR, 15000));
  }

}
