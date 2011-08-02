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

package com.cloudera.flume.master.logical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.antlr.runtime.RecognitionException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.PatternMatch;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.availability.ConsistentHashFailoverChainManager;
import com.cloudera.flume.master.availability.FailoverChainManager;
import com.cloudera.flume.master.failover.FailoverConfigurationManager;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;

/**
 * This test the logical configuration manager, and the logical configuration
 * manager in conjunction with the failover configuration manager.
 */
public class TestLogicalConfigManager {
  public static Logger LOG = Logger.getLogger(TestLogicalConfigManager.class);

  public String DEFAULTFLOW = "default-flow";

  @Before
  public void setDebugLevel() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
    Logger.getLogger(PatternMatch.class).setLevel(Level.INFO);
  }

  ConfigurationManager logical;
  ConfigurationManager trans;
  ConfigurationManager failover;
  StatusManager statman;

  /**
   * Instantiate and expose managers
   */
  void setupNewManagers() {
    ConfigurationManager parent = new ConfigManager();
    ConfigurationManager self = new ConfigManager();
    FailoverChainManager fcMan = new ConsistentHashFailoverChainManager(3);
    ConfigurationManager self2 = new ConfigManager();

    failover = new FailoverConfigurationManager(parent, self2, fcMan);
    statman = new StatusManager();
    logical = new LogicalConfigurationManager(failover, self, statman);
    trans = logical;
  }

  void setupCollectorAgentConfigs() throws IOException, FlumeSpecException {
    // 3 collectors, one auto chain
    trans.setConfig("foo", DEFAULTFLOW, "autoCollectorSource", "null");
    trans.setConfig("foo2", DEFAULTFLOW, "autoCollectorSource", "null");
    trans.setConfig("foo3", DEFAULTFLOW, "autoCollectorSource", "null");
    trans.setConfig("bar", DEFAULTFLOW, "null", "autoBEChain");
  }

  // make it so that the local host info is present
  void setupHeartbeats() {
    String host = NetUtils.localhost();
    statman.updateHeartbeatStatus(host, "physnode", "foo", NodeState.HELLO,
        Clock.unixTime());
    statman.updateHeartbeatStatus(host, "physnode", "foo2", NodeState.HELLO,
        Clock.unixTime());
    statman.updateHeartbeatStatus(host, "physnode", "foo3", NodeState.HELLO,
        Clock.unixTime());
    statman.updateHeartbeatStatus(host, "physnode", "bar", NodeState.HELLO,
        Clock.unixTime());
  }

  // Next mapped logical node to a physical
  void setupLogicalMapping() {
    String host = NetUtils.localhost();
    trans.addLogicalNode(host, "foo");
    trans.addLogicalNode(host, "foo2");
    trans.addLogicalNode(host, "foo3");
    trans.addLogicalNode(host, "bar");
  }

  /**
   * Test the core of the LogicalConfigManager
   */
  @Test
  public void testLogicalTrans() throws IOException, FlumeSpecException {
    ConfigurationManager parent = new ConfigManager();
    ConfigurationManager self = new ConfigManager();
    StatusManager statman = new StatusManager();
    ConfigurationManager trans = new LogicalConfigurationManager(parent, self,
        statman);

    // make it so that the local host info is present
    statman.updateHeartbeatStatus("foo", "foo", "foo", NodeState.HELLO, Clock
        .unixTime());
    statman.updateHeartbeatStatus("bar", "bar", "bar", NodeState.HELLO, Clock
        .unixTime());

    // Next spawn so that all are mapped onto a node and now gets a physical
    trans.addLogicalNode("foo", "foo");
    trans.addLogicalNode("bar", "bar");

    // now set configs
    trans.setConfig("foo", DEFAULTFLOW, "logicalSource", "null");
    trans.setConfig("bar", DEFAULTFLOW, "null", "logicalSink(\"foo\")");

    // update the configurations
    trans.updateAll();
    int port = FlumeConfiguration.get().getCollectorPort();

    // check if translated
    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals("rpcSink( \"foo\", " + port + " )", transData.getSinkConfig());

    FlumeConfigData transData2 = trans.getConfig("foo");
    assertEquals("rpcSource( " + port + " )", transData2.getSourceConfig());
    assertEquals("null", transData2.getSinkConfig());

    // self is same as translated
    FlumeConfigData selfData = self.getConfig("bar");
    assertEquals("null", selfData.getSourceConfig());
    assertEquals("rpcSink( \"foo\", " + port + " )", selfData.getSinkConfig());

    FlumeConfigData selfData2 = self.getConfig("foo");
    assertEquals("rpcSource( " + port + " )", selfData2.getSourceConfig());
    assertEquals("null", selfData2.getSinkConfig());

    // original is the user entered values
    FlumeConfigData origData = parent.getConfig("bar");
    assertEquals("null", origData.getSourceConfig());
    assertEquals("logicalSink(\"foo\")", origData.getSinkConfig());

    FlumeConfigData origData2 = parent.getConfig("foo");
    assertEquals("logicalSource", origData2.getSourceConfig());
    assertEquals("null", origData2.getSinkConfig());
  }

  /**
   * Test interaction between LogicalConfigManager and FailoverConfigManager
   */
  @Test
  public void testFailoverLogicalTrans() throws IOException, FlumeSpecException {
    setupNewManagers();
    setupHeartbeats();
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();
    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    // check the interesting configurations
    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    String lh = NetUtils.localhost();
    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35855 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";
    assertEquals(translated, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // intermediate data
    FlumeConfigData failData = failover.getConfig("bar");
    assertEquals("null", failData.getSourceConfig());
    String failTranslated = "< { lazyOpen => { stubbornAppend => logicalSink( \"foo\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => logicalSink( \"foo3\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => logicalSink( \"foo2\" ) } } > >";
    assertEquals(failTranslated, failData.getSinkConfig());

  }

  /**
   * This method allows tests to be setup with a arrays of strings, and then
   * auto checks the configurations.
   * 
   * Here is the format of the data:
   * 
   * { logical node name, logical source, logical sink, expected source,
   * substring expected in sink }
   **/
  void manyMappingHarness(String[][] lists) throws IOException,
      FlumeSpecException, RecognitionException {
    setupNewManagers();

    String host = NetUtils.localhost();
    // setup nodes
    for (String[] objs : lists) {
      // register nodes in the status manager
      statman.updateHeartbeatStatus(host, "physnode", objs[0], NodeState.HELLO,
          Clock.unixTime());

      // Next spawn so that all are mapped onto a node and now gets a physical
      trans.addLogicalNode(host, objs[0]);

      // set some configs
      trans.setConfig(objs[0], DEFAULTFLOW, objs[1], objs[2]);

    }

    // refresh all the configs so that the logical translations kick in.
    trans.updateAll();

    // check to see physical translations.
    for (String[] objs : lists) {
      FlumeConfigData fcd = trans.getConfig(objs[0]);
      LOG.info(objs[0] + "  " + fcd);
      assertEquals(objs[3], fcd.getSourceConfig());
      LOG.info(fcd + " contains " + objs[4] + "?");
      assertTrue(fcd.getSinkConfig().contains(objs[4]));
      LOG.info(fcd + " does not contain logicalSink?");
      assertTrue(!fcd.getSinkConfig().contains("logicalSink"));
    }
  }

  /**
   * This tests many buried logical sinks going to a single source
   */
  @Test
  public void testSingleSource() throws IOException, FlumeSpecException,
      RecognitionException {
    int port = FlumeConfiguration.get().getCollectorPort();
    String host = NetUtils.localhost();

    String[][] lists = {
        { "foo", "logicalSource", "null", "rpcSource( " + port + " )", "null" },
        { "bar", "null", "logicalSink(\"foo\")", "null",
            "rpcSink( \"" + host + "\", " + port + " )" },
        { "baz", "null", "{ nullDeco => logicalSink(\"foo\") }", "null",
            "rpcSink( \"" + host + "\", " + port + " )" },
        { "baz2", "null", "[ { nullDeco => logicalSink(\"foo\") }, console ]",
            "null", "rpcSink( \"" + host + "\", " + port + " )" },
        { "baz3", "null", "{ nullDeco => < logicalSink(\"foo\") ? null > }",
            "null", "rpcSink( \"" + host + "\", " + port + " )" } };
    manyMappingHarness(lists);
  }

  /**
   * This tests many mappings going to many other sources. Note that foo1 and
   * foo2 are on the same physical node, and have different auto chosen ports.
   */
  @Test
  public void testManySources() throws IOException, FlumeSpecException,
      RecognitionException {
    int foo1port = FlumeConfiguration.get().getCollectorPort();
    int foo2port = foo1port + 1;
    String host = NetUtils.localhost();
    String[][] lists = {
        { "foo1", "logicalSource", "null", "rpcSource( " + foo1port + " )",
            "null" },
        { "foo2", "logicalSource", "null", "rpcSource( " + foo2port + " )",
            "null" },
        { "bar0", "null", "logicalSink(\"foo1\")", "null",
            "rpcSink( \"" + host + "\", " + foo1port + " )" },
        { "bar1", "null", "logicalSink(\"foo2\")", "null",
            "rpcSink( \"" + host + "\", " + foo2port + " )" },
        { "bar2", "null", "[ { nullDeco => logicalSink(\"foo1\") }, console ]",
            "null", "rpcSink( \"" + host + "\", " + foo1port + " )" },
        { "bar3", "null", "{ nullDeco => < logicalSink(\"foo2\") ? null > }",
            "null", "rpcSink( \"" + host + "\", " + foo2port + " " } };
    manyMappingHarness(lists);
  }

  /**
   * This tests many logical nodes on a single logical node.
   */
  @Test
  public void testMultiSink() throws IOException, FlumeSpecException,
      RecognitionException {
    int foo1port = FlumeConfiguration.get().getCollectorPort();
    int foo2port = foo1port + 1;
    String host = NetUtils.localhost();
    String[][] lists = {
        { "foo1", "logicalSource", "null", "rpcSource( " + foo1port + " )",
            "null" },
        { "foo2", "logicalSource", "null", "rpcSource( " + foo2port + " )",
            "null" },
        { "bar0", "null", "logicalSink(\"foo1\")", "null",
            "rpcSink( \"" + host + "\", " + foo1port + " )" },
        { "bar1", "null", "logicalSink(\"foo2\")", "null",
            "rpcSink( \"" + host + "\", " + foo2port + " )" },
        {
            "bar2",
            "null",
            "[ logicalSink(\"foo1\"), logicalSink(\"foo2\"), logicalSink(\"foo1\"), logicalSink(\"foo2\"), console ]",
            "null", "rpcSink( \"" + host + "\", " + foo1port + " )" },
        {
            "bar3",
            "null",
            "< logicalSink(\"foo2\") ? <logicalSink(\"foo1\") ? logicalSink(\"foo1\") > > ",
            "null", "rpcSink( \"" + host + "\", " + foo2port + " " } };
    manyMappingHarness(lists);
  }

  /**
   * This tests many logical nodes on a single logical node going through a
   * roller
   */
  @Test
  public void testRollSink() throws IOException, FlumeSpecException,
      RecognitionException {
    int foo1port = FlumeConfiguration.get().getCollectorPort();
    int foo2port = foo1port + 1;
    String host = NetUtils.localhost();
    String[][] lists = {
        { "foo1", "logicalSource", "null", "rpcSource( " + foo1port + " )",
            "null" },
        { "foo2", "logicalSource", "null", "rpcSource( " + foo2port + " )",
            "null" },
        { "bar0", "null", "logicalSink(\"foo1\")", "null",
            "rpcSink( \"" + host + "\", " + foo1port + " )" },
        { "bar1", "null", "logicalSink(\"foo2\")", "null",
            "rpcSink( \"" + host + "\", " + foo2port + " )" },
        { "bar2", "null",
            "roll(200) { < logicalSink(\"foo1\") ?  logicalSink(\"foo2\") > }",
            "null", "rpcSink( \"" + host + "\", " + foo1port + " )" }, };
    manyMappingHarness(lists);
  }

  /**
   * This tests interaction between LogicalConfigManager and
   * FailoverConfigManager. This verifies that auto*Chains eventually and get
   * translated to physical sinks/sources
   */
  @Test
  public void testAutoChainSink() throws IOException, FlumeSpecException,
      RecognitionException {
    int foo1port = FlumeConfiguration.get().getCollectorPort();
    int foo2port = foo1port + 1;
    String host = NetUtils.localhost();
    String[][] lists = {
        { "foo1", "autoCollectorSource", "null", "rpcSource( 35853 )", "null" },
        { "foo2", "autoCollectorSource", "null", "rpcSource( 35854 )", "null" },
        { "bar0", "null", "autoBEChain", "null",
            "rpcSink( \"" + host + "\", " + foo1port + " )" },
        { "bar1", "null", "autoDFOChain", "null",
            "rpcSink( \"" + host + "\", " + foo2port + " )" },
        { "bar2", "null", "autoE2EChain", "null",
            "rpcSink( \"" + host + "\", " + foo1port + " )" }, };
    manyMappingHarness(lists);
  }

  /**
   * Test interaction between LogicalConfigManager and FailoverConfigManager
   */
  @Test
  public void testMultipleRefreshes() throws IOException, FlumeSpecException {
    setupNewManagers();
    setupHeartbeats();
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();

    FlumeConfigData transData0 = trans.getConfig("bar");
    assertEquals("null", transData0.getSourceConfig());
    String lh = NetUtils.localhost();
    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35855 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";
    assertEquals(translated, transData0.getSinkConfig());

    trans.updateAll();

    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    trans.updateAll();

    transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    trans.updateAll();

    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // intermediate data
    FlumeConfigData failData = failover.getConfig("bar");
    assertEquals("null", failData.getSourceConfig());
    String failTranslated = "< { lazyOpen => { stubbornAppend => logicalSink( \"foo\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => logicalSink( \"foo3\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => logicalSink( \"foo2\" ) } } > >";
    assertEquals(failTranslated, failData.getSinkConfig());
  }

  /**
   * Test incremental version stamp updating with no changes.
   */
  @Test
  public void testMultipleUpdates() throws IOException, FlumeSpecException {
    setupNewManagers();
    setupHeartbeats();
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();

    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    FlumeConfigData transData0 = trans.getConfig("bar");
    assertEquals("null", transData0.getSourceConfig());
    String lh = NetUtils.localhost();
    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35855 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";
    assertEquals(translated, transData0.getSinkConfig());

    trans.updateAll();

    FlumeConfigData transData = trans.getConfig("bar");
    long barVer = transData.getTimestamp();
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    long fooVer = transCollData.getTimestamp();
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    trans.updateAll();

    transData = trans.getConfig("bar");
    assertEquals(barVer, transData.getTimestamp());
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals(fooVer, transCollData.getTimestamp());
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    trans.updateAll(); // TODO (jon) Ideally, this shouldn't be necessary

    LOG.info("Full Translation: " + trans);
    LOG.info("Logical Translation: " + logical);
    LOG.info("Failover Translation: " + failover);

    transData = trans.getConfig("bar");
    assertEquals(barVer, transData.getTimestamp());
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals(fooVer, transCollData.getTimestamp());
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // intermediate data
    FlumeConfigData failData = failover.getConfig("bar");
    assertEquals("null", failData.getSourceConfig());
    String failTranslated = "< { lazyOpen => { stubbornAppend => logicalSink( \"foo\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => logicalSink( \"foo3\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => logicalSink( \"foo2\" ) } } > >";
    assertEquals(failTranslated, failData.getSinkConfig());
  }

  /**
   * Test changes to configuration and incremental version stamp changes.
   */
  @Test
  public void testUnconfigures() throws IOException, FlumeSpecException {
    setupNewManagers();
    setupHeartbeats();
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // Look, no explicit update to the configurations!

    // foo3 is no longer a collector
    long fooVer = trans.getConfig("foo").getTimestamp();
    long foo2Ver = trans.getConfig("foo2").getTimestamp();
    long foo3Ver = trans.getConfig("foo3").getTimestamp();
    long barVer = trans.getConfig("bar").getTimestamp();
    trans.setConfig("foo3", DEFAULTFLOW, "null", "null");

    // Look, no explicit update to the configurations!

    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    String lh = NetUtils.localhost();
    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } >";
    assertEquals(translated, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    assertEquals(fooVer, trans.getConfig("foo").getTimestamp());
    assertEquals(foo2Ver, trans.getConfig("foo2").getTimestamp());
    assertNotSame(foo3Ver, trans.getConfig("foo3").getTimestamp());
    foo3Ver = trans.getConfig("foo3").getTimestamp();
    assertNotSame(barVer, trans.getConfig("bar").getTimestamp());
    barVer = trans.getConfig("bar").getTimestamp();

    // intermediate data
    FlumeConfigData failData = failover.getConfig("bar");
    assertEquals("null", failData.getSourceConfig());
    String failTranslated = "< { lazyOpen => { stubbornAppend => logicalSink( \"foo\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => logicalSink( \"foo2\" ) } } >";
    assertEquals(failTranslated, failData.getSinkConfig());

    assertEquals(fooVer, trans.getConfig("foo").getTimestamp());
    assertEquals(foo2Ver, trans.getConfig("foo2").getTimestamp());
    assertEquals(foo3Ver, trans.getConfig("foo3").getTimestamp());
    assertEquals(barVer, trans.getConfig("bar").getTimestamp());
  }

  /**
   * This initially has many logical nodes, that are not mapped to a physical
   * node (never heartbeated). We then "trigger" a heart beat adding one at a
   * time and watch the translated configuration change.
   * 
   * Any failed translations are wrapped with a 'fail' sink which just throws
   * exceptions. These are still valid configurations, and will just failover if
   * in a failover configuration.
   */
  @Test
  public void testNoPhysicalNode() throws IOException, FlumeSpecException {
    setupNewManagers();

    // NO local host information present to make physical node!
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();

    FlumeConfigData transData0 = trans.getConfig("bar");
    assertEquals("null", transData0.getSourceConfig());
    String lh = NetUtils.localhost();
    String first = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo\\\" )\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    assertEquals(first, transData0.getSinkConfig());

    // update one at a time and check config
    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo",
        NodeState.HELLO, Clock.unixTime());
    trans.updateAll(); // TODO remove
    String second = "< { lazyOpen => { stubbornAppend => rpcSink( \""
        + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(second, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // update one at a time and check config
    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo2",
        NodeState.HELLO, Clock.unixTime());
    trans.updateAll();
    String third = "< { lazyOpen => { stubbornAppend => rpcSink( \""
        + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";
    transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(third, transData.getSinkConfig());

    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35855 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo3",
        NodeState.HELLO, Clock.unixTime());
    trans.updateAll();

    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // intermediate data
    FlumeConfigData failData = failover.getConfig("bar");
    assertEquals("null", failData.getSourceConfig());
    String failTranslated = "< { lazyOpen => { stubbornAppend => logicalSink( \"foo\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => logicalSink( \"foo3\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => logicalSink( \"foo2\" ) } } > >";
    assertEquals(failTranslated, failData.getSinkConfig());
  }

  /**
   * We knock out a node. What is the right behavior? Use old information.
   */

  @Test
  public void testPhysicalNodeLost() throws IOException, FlumeSpecException {
    setupNewManagers();
    setupLogicalMapping();
    // No local host information present to make physical node!
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();

    FlumeConfigData transData0 = trans.getConfig("bar");
    assertEquals("null", transData0.getSourceConfig());
    String lh = NetUtils.localhost();
    String first = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo\\\" )\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    assertEquals(first, transData0.getSinkConfig());

    // update one at a time and check config
    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo",
        NodeState.HELLO, Clock.unixTime());
    trans.updateAll(); // TODO remove
    String second = "< { lazyOpen => { stubbornAppend => rpcSink( \""
        + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(second, transData.getSinkConfig());

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // We knock out a node. What is the right behavior? Use old information.
    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo",
        NodeState.LOST, Clock.unixTime());
    trans.updateAll(); // TODO remove
    String third = "< { lazyOpen => { stubbornAppend => rpcSink( \""
        + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(third, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());
  }

  /**
   * An unmap command should force the logical sources/sinks to revert to failed
   * state.
   */
  @Test
  public void testUpdateAfterUnmapAll() throws IOException, FlumeSpecException {

    setupNewManagers();
    setupHeartbeats();
    setupLogicalMapping();
    setupCollectorAgentConfigs();

    // update the configurations
    trans.updateAll();
    String lh = NetUtils.localhost();
    String translated = "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35853 ) } } ? "
        + "< { lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35855 ) } } ? "
        + "{ lazyOpen => { stubbornAppend => rpcSink( \"" + lh
        + "\", 35854 ) } } > >";

    FlumeConfigData transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    statman.updateHeartbeatStatus(NetUtils.localhost(), "physnode", "foo3",
        NodeState.HELLO, Clock.unixTime());
    trans.updateAll();

    LOG.info("Full Translation: " + trans);
    LOG.info("logical Translation: " + logical);
    LOG.info("failover Translation: " + failover);

    FlumeConfigData transData = trans.getConfig("bar");
    assertEquals("null", transData.getSourceConfig());
    assertEquals(translated, transData.getSinkConfig());

    transCollData = trans.getConfig("foo");
    assertEquals("rpcSource( 35853 )", transCollData.getSourceConfig());
    assertEquals("null", transCollData.getSinkConfig());

    // //
    // Unmap all the nodes.
    // //
    trans.unmapAllLogicalNodes();

    // This should go back to the failed state
    FlumeConfigData transData0 = trans.getConfig("bar");
    assertEquals("null", transData0.getSourceConfig());
    String first = "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo\\\" )\" ) } } ? "
        + "< { lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo3\\\" )\" ) } } ? "
        + "{ lazyOpen => { stubbornAppend => fail( \"logicalSink( \\\"foo2\\\" )\" ) } } > >";
    assertEquals(first, transData0.getSinkConfig());

  }

}
