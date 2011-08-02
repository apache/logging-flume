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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.availability.ConsistentHashFailoverChainManager;
import com.cloudera.flume.master.availability.FailoverChainManager;
import com.cloudera.flume.master.failover.FailoverConfigurationManager;
import com.cloudera.flume.master.logical.LogicalConfigurationManager;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;

/**
 * This tests all kinds of auto update scenarios where a user or new machine
 * update causes an incremental update to some machine configurations.
 * 
 */
public class TestMasterAutoUpdates {
  final public static Logger LOG = Logger
      .getLogger(TestMasterAutoUpdates.class);

  // /////////

  protected FlumeMaster flumeMaster = null;
  private File tmpdir = null;
  protected ConfigManager cfgMan;

  /**
   * This creates an environment where we have configurations set and then
   * serving starts. This simulates a zk configstore load and then the serve
   * call being run.
   * 
   * Ideally we'd create a SetupTranslatingZKMasterTestEnv, but there is an
   * issue when trying to start/shutdown and start a new master in the same
   * process/jvm.
   * */
  @Before
  public void setCfgAndStartMaster() throws TTransportException, IOException,
      FlumeSpecException {
    // Give ZK a temporary directory, otherwise it's possible we'll reload some
    // old configs
    tmpdir = FileUtil.mktempdir();
    FlumeConfiguration.createTestableConfiguration();
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_STORE, "memory");

    buildMaster();

    // Instead of loading from a ZK Store, we just see the config in the "deep"
    // config manager. Any translations will not occur.
    ConfigurationManager loaded = cfgMan;
    loaded.setConfig("node1", "flow", "autoCollectorSource", "null");
    loaded.setConfig("node2", "flow", "autoCollectorSource", "null");
    loaded.setConfig("node3", "flow", "autoCollectorSource", "null");
    loaded.setConfig("node4", "flow", "autoCollectorSource", "null");
    loaded.setConfig("agent", "flow", "null", "autoBEChain");

    // this is the outer configman, should have no translation.
    ConfigurationManager cfgman1 = flumeMaster.getSpecMan();
    Map<String, FlumeConfigData> cfgs1 = cfgman1.getTranslatedConfigs();
    assertEquals(0, cfgs1.size()); // no translations happened

    // start the master (which should trigger an update and translation
    flumeMaster.serve();
  }

  /**
   * Build but do not start a master.
   * 
   * This exposes a hook to the deepest cfgMan which would ideally be a saved ZK
   * backed version being reloaded from a restarted master.
   */
  void buildMaster() throws IOException {
    cfgMan = new ConfigManager(FlumeMaster.createConfigStore(FlumeConfiguration
        .get()));
    FailoverChainManager fcMan = new ConsistentHashFailoverChainManager(3);
    ConfigurationManager self2 = new ConfigManager();
    ConfigurationManager failover = new FailoverConfigurationManager(cfgMan,
        self2, fcMan);

    StatusManager statman = new StatusManager();
    ConfigurationManager self = new ConfigManager();
    ConfigurationManager logical = new LogicalConfigurationManager(failover,
        self, statman);
    flumeMaster = new FlumeMaster(new CommandManager(), logical, statman,
        new MasterAckManager(), FlumeConfiguration.get());

  }

  @After
  public void stopMaster() throws IOException {
    if (flumeMaster != null) {
      flumeMaster.shutdown();
      flumeMaster = null;
    }

    if (tmpdir != null) {
      FileUtil.rmr(tmpdir);
      tmpdir = null;
    }
  }

  // /////// end stuff that should be refactored

  /**
   * Ideally, start a master (calling serve), set a configuration, kill the
   * master, and then reload it. We make sure that there was an attempt to
   * translate the configuration. This simulates a ZK-backed master going down
   * and coming back up with the previously specfied configuration.
   */
  @Test
  public void testReloadRefresh() throws IOException, InterruptedException,
      FlumeSpecException {

    ConfigurationManager cfgman2 = flumeMaster.getSpecMan();
    Map<String, FlumeConfigData> cfgs2 = cfgman2.getTranslatedConfigs();
    assertEquals(5, cfgs2.size());
  }

  /**
   * The configuration here has no live nodes. It translates the failchains but
   * fail on logicalSink translations. This is the base case for most of the
   * subsequent tests.
   */
  @Test
  public void testMasterNoNode() {
    Map<String, FlumeConfigData> xcfgs = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd = xcfgs.get("agent");
    String ans1 = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node4\\\" )\" ) } } ?"
        + " < { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node2\\\" )\" ) } } ?"
        + " { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node1\\\" )\" ) } } > >";
    assertEquals(agentFcd.sinkConfig, ans1);
  }

  /**
   * A user triggered reconfigure of a collector to a non collector should cause
   * a configuration that depends on the removed configuration to be removed.
   */
  @Test
  public void testCollectorReconfigAutoUpdate() throws IOException,
      FlumeSpecException {

    // a user initiated removal of a node would cause the config to change.
    flumeMaster.getSpecMan().setConfig("node2", "flow", "null", "null");

    // Look, no explicit updates!

    // check new config
    Map<String, FlumeConfigData> xcfgs2 = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd2 = xcfgs2.get("agent");
    String ans2 = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node4\\\" )\" ) } } ?"
        + " < { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node1\\\" )\" ) } } ?"
        + " { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node3\\\" )\" ) } } > >";
    assertEquals(agentFcd2.sinkConfig, ans2);
  }

  /**
   * A user triggered decommission should cause a configuration that depends on
   * the removed configuration to be removed.
   */
  @Test
  public void testDecommission() throws IOException {

    // a user initiated removal of a node would cause the config to change.
    flumeMaster.getSpecMan().removeLogicalNode("node2");

    // Look, no explicit update call!

    // check new config
    Map<String, FlumeConfigData> xcfgs2 = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd2 = xcfgs2.get("agent");
    // This is wrong -- there should be a different logicalSink replacing node2
    String ans2 = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node4\\\" )\" ) } } ?"
        + " < { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node1\\\" )\" ) } } ?"
        + " { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node3\\\" )\" ) } } > >";
    assertEquals(agentFcd2.sinkConfig, ans2);
  }

  /**
   * Add a new collectorSource node, and make sure the agent's configuration is
   * updated.
   */
  @Test
  public void testMasterNodeNewCollectorAutoUpdate() throws IOException,
      FlumeSpecException {

    // a user initiated removal of a node would cause the config to change.
    flumeMaster.getSpecMan().setConfig("nodeNew", "flow",
        "autoCollectorSource", "null");

    // Look, no explicit update call!

    // check new config
    Map<String, FlumeConfigData> xcfgs2 = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd2 = xcfgs2.get("agent");
    String ans2 = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"nodeNew\\\" )\" ) } } ?"
        + " < { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node4\\\" )\" ) } } ?"
        + " { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node2\\\" )\" ) } } > >";
    assertEquals(agentFcd2.sinkConfig, ans2);

  }

  /**
   * Test that an autoUpdate happens when a physical node information
   * (heartbeat) shows up and allows for a logicalSink/Source translation
   * 
   * This condition is assumed in the following test --
   * testMasterNodeUnmapAutoUpdate()
   */

  @Test
  public void testMasterNodeAutoUpdate() throws IOException, FlumeSpecException {

    // First, heart beats
    String host = NetUtils.localhost();
    long ver = Clock.unixTime();
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node1",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node2",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node3",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node4",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "agent",
        NodeState.IDLE, ver);

    // Next spawn so that all are mapped onto a node and now gets a physical
    flumeMaster.getSpecMan().addLogicalNode(host, "node1");
    flumeMaster.getSpecMan().addLogicalNode(host, "node2");
    flumeMaster.getSpecMan().addLogicalNode(host, "node3");
    flumeMaster.getSpecMan().addLogicalNode(host, "node4");
    flumeMaster.getSpecMan().addLogicalNode(host, "agent");

    // Look, no explicit update call!

    // check new config
    Map<String, FlumeConfigData> xcfgs2 = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd2 = xcfgs2.get("agent");
    // This is wrong -- there should be a different logicalSink replacing node2
    String ans2 = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + host
        + "\", 35856 ) } } ?"
        + " < { lazyOpen => { stubbornAppend => rpcSink( \"" + host
        + "\", 35854 ) } } ?"
        + " { lazyOpen => { stubbornAppend => rpcSink( \"" + host
        + "\", 35853 ) } } > >";
    assertEquals(ans2, agentFcd2.sinkConfig);
  }

  /**
   * This heartbeats to provide physical node info and allows the translators to
   * build fully physical configurations.
   */
  @Test
  public void testMasterNodeUnmapAutoUpdate() throws IOException {

    // First, heart beats
    String host = NetUtils.localhost();
    long ver = Clock.unixTime();
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node1",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node2",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node3",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "node4",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "physnode", "agent",
        NodeState.IDLE, ver);

    // First, spawn so that all are mapped onto a node and now gets a physical
    // node info
    flumeMaster.getSpecMan().addLogicalNode("host", "node1");
    flumeMaster.getSpecMan().addLogicalNode("host", "node2");
    flumeMaster.getSpecMan().addLogicalNode("host", "node3");
    flumeMaster.getSpecMan().addLogicalNode("host", "node4");
    flumeMaster.getSpecMan().addLogicalNode("host", "agent");

    // Now do a user initiated unmap should make the config go back to a failing
    // version with logicalSinks
    flumeMaster.getSpecMan().unmapAllLogicalNodes();

    // Look, no explicit update call!

    // check new config
    Map<String, FlumeConfigData> xcfgs2 = flumeMaster.getSpecMan()
        .getTranslatedConfigs();
    FlumeConfigData agentFcd2 = xcfgs2.get("agent");
    String ans2 = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node4\\\" )\" ) } } ?"
        + " < { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node2\\\" )\" ) } } ?"
        + " { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"node1\\\" )\" ) } } > >";
    assertEquals(ans2, agentFcd2.sinkConfig);

  }

}
