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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.CommandManager;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.MasterAckManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.flows.FlowConfigManager;
import com.cloudera.flume.master.logical.LogicalConfigurationManager;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;

/**
 * This tests the flow config manager that have isolated failoverchain
 * translators and then logical node translations.
 */
public class TestLogicalFailoverFlowConfigManager {
  final public static Logger LOG = Logger
      .getLogger(TestLogicalFailoverFlowConfigManager.class);
  protected FlumeMaster flumeMaster = null;
  private File tmpdir = null;
  protected ConfigManager cfgMan;
  protected FlowConfigManager flowed;
  protected LogicalConfigurationManager logical;

  @Before
  public void setDebug() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

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
    loaded.setConfig("coll11", "flow1", "autoCollectorSource", "null");
    loaded.setConfig("coll12", "flow1", "autoCollectorSource", "null");
    loaded.setConfig("coll13", "flow1", "autoCollectorSource", "null");
    loaded.setConfig("coll14", "flow1", "autoCollectorSource", "null");
    loaded.setConfig("agent1", "flow1", "null", "autoBEChain");

    loaded.setConfig("coll21", "flow2", "autoCollectorSource", "null");
    loaded.setConfig("coll22", "flow2", "autoCollectorSource", "null");
    loaded.setConfig("coll23", "flow2", "autoCollectorSource", "null");
    loaded.setConfig("coll24", "flow2", "autoCollectorSource", "null");
    loaded.setConfig("agent2", "flow2", "null", "autoBEChain");

    // this is the outer configman, should have no translation.
    ConfigurationManager cfgman1 = flumeMaster.getSpecMan();
    Map<String, FlumeConfigData> cfgs1 = cfgman1.getTranslatedConfigs();
    assertEquals(0, cfgs1.size()); // no translations happened

    // start the master (which should trigger an update and translation
    flumeMaster.serve();

    // First, heart beats
    String host = NetUtils.localhost();
    long ver = Clock.unixTime();
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll11",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll12",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll13",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll14",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "agent1",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll21",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll22",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll23",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "coll24",
        NodeState.IDLE, ver);
    flumeMaster.getStatMan().updateHeartbeatStatus(host, "phys", "agent2",
        NodeState.IDLE, ver);

    // First, spawn so that all are mapped onto a node and now gets a physical
    // node info
    flumeMaster.getSpecMan().addLogicalNode("host", "coll11");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll12");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll13");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll14");
    flumeMaster.getSpecMan().addLogicalNode("host", "agent1");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll21");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll22");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll23");
    flumeMaster.getSpecMan().addLogicalNode("host", "coll24");
    flumeMaster.getSpecMan().addLogicalNode("host", "agent2");
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
    StatusManager statman = new StatusManager();
    flowed = new FlowConfigManager.FailoverFlowConfigManager(cfgMan, statman);
    logical = new LogicalConfigurationManager(flowed, new ConfigManager(),
        statman);
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

  /**
   * now that we have some nodes, do the full translation and then check to see
   * if configurations are changed consistently.
   */
  @Test
  public void testCollectorMove() throws IOException, FlumeSpecException {
    assertEquals(5, flowed.getConfigManForFlow("flow1").getTranslatedConfigs()
        .size());
    assertEquals(5, flowed.getConfigManForFlow("flow2").getTranslatedConfigs()
        .size());
    assertEquals(5, flowed.getConfigManForFlow("flow1").getAllConfigs().size());
    assertEquals(5, flowed.getConfigManForFlow("flow2").getAllConfigs().size());
    assertEquals(10, flowed.getTranslatedConfigs().size());

    FlumeConfigData agent1 = logical.getConfig("agent1");
    FlumeConfigData agent2 = logical.getConfig("agent2");

    String host = NetUtils.localhost();
    LOG.info(agent1);
    assertEquals("< { lazyOpen => rpcSink( \"" + host + "\", 35856 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35853 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35855 ) } ? null > > >",
        agent1.sinkConfig);
    LOG.info(agent2);
    assertEquals("< { lazyOpen => rpcSink( \"" + host + "\", 35859 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35858 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35857 ) } ? null > > >",
        agent2.sinkConfig);

    // change the flow group of one of the relevent logicalSinks.
    logical.setConfig("coll14", "flow2", "autoCollectorSource", "null");
    FlumeConfigData nextAgent1 = logical.getConfig("agent1");
    FlumeConfigData nextAgent2 = logical.getConfig("agent2");

    // look, the coll14 was removed from agent1 and into agent2
    LOG.info(nextAgent1);
    assertEquals("< { lazyOpen => rpcSink( \"" + host + "\", 35853 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35855 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35854 ) } ? null > > >",
        nextAgent1.sinkConfig);
    LOG.info(nextAgent2);
    assertEquals("< { lazyOpen => rpcSink( \"" + host + "\", 35859 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35858 ) } ? "
        + "< { lazyOpen => rpcSink( \"" + host + "\", 35856 ) } ? null > > >",
        nextAgent2.sinkConfig);
  }

}
