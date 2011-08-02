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

package com.cloudera.flume.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.thrift.transport.TTransportException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.SetupMasterTestEnv;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.flume.util.FlumeShell;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * Test a few basic commands on the flume shell.
 */
public class TestFlumeShell extends SetupMasterTestEnv {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestFlumeShell.class);

  /**
   * Start a master, connect to it via the shell, and then issue a
   * configuration.
   */
  @Test
  public void testConnectConfig() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    FlumeShell sh = new FlumeShell();

    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    sh.executeLine("exec config localhost null null");
    Clock.sleep(250);
    assertTrue(flumeMaster.getSpecMan().getAllConfigs().size() > 0);
  }

  /**
   * Start a master, connect to it via the shell, and then issue a setChokeLimit
   * and make sure the chokeMap at the master is updated.
   */
  @Test
  public void testSetChokeLimit() throws InterruptedException,
      TTransportException, IOException {

    FlumeShell sh = new FlumeShell();

    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    sh.executeLine("exec setChokeLimit physNode choke 786");
    Clock.sleep(250);
    assertEquals(786, flumeMaster.getSpecMan().getChokeMap("physNode").get(
        "choke").intValue());
  }

  /**
   * Create a master, then connect via shell, and then issue multi
   * configuration.
   */
  @Test
  public void testConnectMultiConfig() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    FlumeShell sh = new FlumeShell();
    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    sh.executeLine("exec multiconfig 'localhost:text(\"/var/log/messages\") | "
        + "{delay (250) => console(\"avrojson\") };'");
    Clock.sleep(250);
    assertTrue(flumeMaster.getSpecMan().getAllConfigs().size() > 0);
  }

  /**
   * Create a master, connect via shell, create some logical nodes, save the
   * config for the node and check if the output looks as expected.
   */
  @Test
  public void testSaveConfigCommand() throws IOException {
    FlumeShell sh = new FlumeShell();
    long retval;

    retval = sh.executeLine("connect localhost:"
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    assertEquals(0, retval);

    retval = sh.executeLine("exec config foo 'null' 'console'");
    assertEquals(0, retval);

    File saveFile = FileUtil.createTempFile("test-flume", "");
    saveFile.delete();
    saveFile.deleteOnExit();

    retval = sh.executeLine("exec save '" + saveFile.getAbsolutePath() + "'");
    assertEquals(0, retval);

    BufferedReader in = new BufferedReader(new FileReader(saveFile));
    assertEquals("foo : null | console;", in.readLine());
    assertNull(in.readLine());
    in.close();
  }

  /**
   * Create a master, create a config file, connect via shell, load config file,
   * compare if flow looks as expected in FlumeConfigData.
   */
  @Test
  public void testLoadConfigCommand() throws IOException {
    FlumeShell sh = new FlumeShell();
    long retval;

    retval = sh.executeLine("connect localhost:"
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    assertEquals(0, retval);

    File saveFile = FileUtil.createTempFile("test-flume", "");
    saveFile.deleteOnExit();
    BufferedWriter out = new BufferedWriter(new FileWriter(saveFile));
    out.write("foo : null | console;\n");
    out.close();

    retval = sh.executeLine("exec load '" + saveFile.getAbsolutePath() + "'");
    assertEquals(0, retval);

    ConfigurationManager manager = flumeMaster.getSpecMan();
    FlumeConfigData data = manager.getConfig("foo");
    assertEquals(data.getSinkConfig(), "console");
    assertEquals(data.getSourceConfig(), "null");
  }

  /**
   * Create a master, connect via shell, create some logical nodes, spawn them,
   * and see if the output looks as expected.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testGetMappings() throws InterruptedException {
    FlumeShell sh = new FlumeShell();
    long retval;

    retval = sh.executeLine("connect localhost:"
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    assertEquals(0, retval);

    retval = sh.executeLine("getmappings");
    assertEquals(0, retval);

    assertEquals(0, flumeMaster.getSpecMan().getLogicalNodeMap().size());

    Clock.sleep(1000);

    retval = sh
        .executeLine("exec config foo 'tail(\"/var/log/messages\")' 'console(\"avrojson\")'");
    assertEquals(0, retval);
    retval = sh
        .executeLine("exec config bar 'tail(\"/var/log/messages2\")' 'console(\"avrojson\")'");
    assertEquals(0, retval);

    retval = sh.executeLine("exec spawn localhost foo");
    assertEquals(0, retval);
    assertEquals(1, flumeMaster.getSpecMan().getLogicalNodeMap().size());

    retval = sh.executeLine("getmappings");
    assertEquals(0, retval);

    retval = sh.executeLine("exec spawn localhost bar");
    assertEquals(0, retval);
    assertEquals(2, flumeMaster.getSpecMan().getLogicalNodeMap().size());

    retval = sh.executeLine("getmappings");
    assertEquals(0, retval);

    retval = sh.executeLine("getmappings idonotexist");
    assertEquals(0, retval);

    retval = sh.executeLine("getmappings localhost");
    assertEquals(0, retval);
  }

  /**
   * Create a master, then connect via shell, and then issue a long noop
   */
  @Test
  public void testCommitTimeOut() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    FlumeShell sh = new FlumeShell();
    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    long start = Clock.unixTime();
    // wait for a ridiculous amount of time, and force timeout to happen.
    sh.executeLine("exec noop 20000");

    long delta = Clock.unixTime() - start;
    assertTrue(delta < 20000);
    assertTrue(delta > FlumeShell.CMD_WAIT_TIME_MS);
  }

  /**
   * Create a master, then connect via shell, and then issue multi
   * configuration.
   */
  @Test
  public void testSubmitWait() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    FlumeShell sh = new FlumeShell();
    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    long start = Clock.unixTime();
    // wait for a ridiculous amount of time, and force timeout to happen.
    sh.executeLine("submit noop 3000");
    long delta = Clock.unixTime() - start;

    assertTrue(delta < 3000);

    // implicit wait.
    sh.executeLine("wait");
    delta = Clock.unixTime() - start;
    assertTrue(delta > 3000);
  }

  /**
   * Create a master and a node, then connect via shell, and then issue a
   * configuration, get the node the configuration, and then wait until the
   * config has finished.
   */
  @Test
  public void testNodesDone() throws InterruptedException, TTransportException,
      IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    String nodename = "bar";
    flumeMaster.getSpecMan().addLogicalNode(nodename, "foo");
    FlumeNode n = new FlumeNode(FlumeConfiguration.get(), nodename,
        new DirectMasterRPC(flumeMaster), false, false);
    n.start();

    // jumpstart the heartbeat (get foo register, and its default logicalNode
    // started)
    n.getLivenessManager().heartbeatChecks();

    // One for the logical node by default, one for foo
    assertEquals(2, flumeMaster.getStatMan().getNodeStatuses().size());

    FlumeShell sh = new FlumeShell();
    sh.executeLine("connect localhost: "
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    sh
        .executeLine("exec config foo 'synth(100)' '{delay(100) => accumulator(\"count\") }' ");

    FlumeConfigData fcd = flumeMaster.getSpecMan().getConfig("foo");
    assertEquals("{delay(100) => accumulator(\"count\") }", fcd.sinkConfig);
    assertEquals("synth(100)", fcd.sourceConfig);
    assertTrue(0 != fcd.timestamp);

    sh.executeLine("waitForNodesDone 0 foo");
    n.getLivenessManager().heartbeatChecks();
    NodeState status = flumeMaster.getStatMan().getNodeStatuses().get("foo").state;
    NodeState idle = NodeState.IDLE;
    assertEquals(status, idle);
    // TODO: uncomment when there is a clean way to get at the reportable
    n.stop();
  }

  /**
   * Create a master and 3 nodes, then connect via shell, and then issue
   * configurations, get the nodes a configuration, and then wait until the
   * configs has finished.
   */
  @Test
  @Ignore("Can't get a handle to the countersink, must fix")
  public void test3NodesDone() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    String nodename = "foo";
    FlumeConfiguration conf = FlumeConfiguration.createTestableConfiguration();
    FlumeNode n = new FlumeNode(conf, nodename,
        new DirectMasterRPC(flumeMaster), false, false);
    n.start();

    String node2 = "bar";
    FlumeNode n2 = new FlumeNode(conf, node2, new DirectMasterRPC(flumeMaster),
        false, false);
    n2.start();

    String node3 = "baz";
    FlumeNode n3 = new FlumeNode(conf, node3, new DirectMasterRPC(flumeMaster),
        false, false);
    n3.start();

    // jumpstart the heartbeat (get foo register, and its default logicalNode
    // started)
    n.getLivenessManager().heartbeatChecks();
    n2.getLivenessManager().heartbeatChecks();
    n3.getLivenessManager().heartbeatChecks();

    assertEquals(3, flumeMaster.getStatMan().getNodeStatuses().size());

    FlumeShell sh = new FlumeShell();
    sh.executeLine("connect localhost: "
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    sh
        .executeLine("exec config foo 'synth(100)' '{delay(100) => accumulator(\"count\") }' ");
    sh
        .executeLine("exec config bar 'synth(50)' '{delay(100) => accumulator(\"count2\") }' ");
    sh
        .executeLine("exec config baz 'synth(75)' '{delay(100) => accumulator(\"count3\") }' ");

    FlumeConfigData fcd = flumeMaster.getSpecMan().getConfig("foo");
    assertEquals("{delay(100) => accumulator(\"count\") }", fcd.sinkConfig);
    assertEquals("synth(100)", fcd.sourceConfig);
    assertTrue(0 != fcd.timestamp);

    sh.executeLine("waitForNodesDone 0 foo bar baz");
    n.getLivenessManager().heartbeatChecks();
    NodeState status = flumeMaster.getStatMan().getNodeStatuses().get(nodename).state;
    NodeState idle = NodeState.IDLE;
    assertEquals(status, idle);
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(100, cnt.getCount());
    AccumulatorSink cnt2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    assertEquals(50, cnt2.getCount());
    AccumulatorSink cnt3 = (AccumulatorSink) ReportManager.get().getReportable(
        "count3");
    assertEquals(75, cnt3.getCount());
    n.stop();
    n2.stop();
    n3.stop();
  }

  /**
   * Create a master and a node, then connect via shell, and then issue a
   * configuration, get the node the configuration, and then wait until the
   * config has finished.
   */
  @Test
  @Ignore("Can't get a handle to the countersink, must fix")
  public void testNodesActive() throws InterruptedException,
      TTransportException, IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    String nodename = "foo";
    FlumeNode n = new FlumeNode(FlumeConfiguration.get(), nodename,
        new DirectMasterRPC(flumeMaster), false, false);
    n.start();

    // jumpstart the heartbeat (get foo register, and its default logicalNode
    // started)
    n.getLivenessManager().heartbeatChecks();

    assertEquals(1, flumeMaster.getStatMan().getNodeStatuses().size());

    FlumeShell sh = new FlumeShell();
    sh.executeLine("connect localhost: "
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);
    // this will run for 10 seconds
    sh
        .executeLine("exec config foo 'synth(100)' '{delay(100) => accumulator(\"count\") }' ");

    FlumeConfigData fcd = flumeMaster.getSpecMan().getConfig("foo");
    assertEquals("{delay(100) => accumulator(\"count\") }", fcd.sinkConfig);
    assertEquals("synth(100)", fcd.sourceConfig);
    assertTrue(0 != fcd.timestamp);

    sh.executeLine("waitForNodesActive 0 foo");
    n.getLivenessManager().heartbeatChecks();
    NodeStatus status = flumeMaster.getStatMan().getNodeStatuses()
        .get(nodename);
    NodeState active = NodeState.ACTIVE;
    assertEquals(status.state, active);

    sh.executeLine("waitForNodesDone 0 foo");
    n.getLivenessManager().heartbeatChecks();
    status = flumeMaster.getStatMan().getNodeStatuses().get(nodename);
    NodeState idle = NodeState.IDLE;
    assertEquals(status.state, idle);
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(100, cnt.getCount());
    n.stop();
  }

  @Test
  public void testGetCommandStatus() throws IOException {
    PrintStream olderr = System.err;
    ByteArrayOutputStream tmpErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(tmpErr));

    FlumeShell sh = new FlumeShell();
    sh.executeLine("connect localhost: "
        + FlumeConfiguration.DEFAULT_ADMIN_PORT);

    // this is a bad command line
    long task = sh.executeLine("exec config foo 'blah' "
        + "'{delay(100) => accumulator(\"count\") }' ");

    // restore err
    System.setErr(olderr);
    ByteArrayInputStream bais = new ByteArrayInputStream(tmpErr.toByteArray());
    BufferedReader bis = new BufferedReader(new InputStreamReader(bais));

    int count = 0;
    while (bis.ready()) {
      String line = bis.readLine();
      count++;
      LOG.error(line);
      assertTrue(line.equals("Command failed")
          || line.equals("Attempted to write an invalid sink/source:"
              + " Invalid source: blah"));
    }
    assertEquals(2, count);

  }

  /**
   * Start a master, connect to it via the shell, fake a heartbeat, purge the
   * heartbeat, fake a few more heartbeats, purge all the heartbeats
   */
  @Test
  public void testPurge() throws InterruptedException, TTransportException,
      IOException {
    assertEquals(0, flumeMaster.getSpecMan().getAllConfigs().size());

    FlumeShell sh = new FlumeShell();
    sh
        .executeLine("connect localhost:"
            + FlumeConfiguration.DEFAULT_ADMIN_PORT);

    flumeMaster.getStatMan().updateHeartbeatStatus("foo", "foo", "foo",
        NodeState.HELLO, 1);
    assertNotNull(flumeMaster.getStatMan().getStatus("foo"));

    // no exception when called with node that doesn't exist.
    sh.executeLine("exec purge wackadoodle");

    // successful case
    sh.executeLine("exec purge foo");
    Clock.sleep(250);

    NodeStatus stat = flumeMaster.getStatMan().getStatus("foo");
    assertNull(stat);

    flumeMaster.getStatMan().updateHeartbeatStatus("foo", "foo", "foo",
        NodeState.HELLO, 1);
    flumeMaster.getStatMan().updateHeartbeatStatus("bar", "bar", "bar",
        NodeState.HELLO, 1);
    flumeMaster.getStatMan().updateHeartbeatStatus("baz", "baz", "baz",
        NodeState.HELLO, 1);

    assertNotNull(flumeMaster.getStatMan().getStatus("foo"));
    assertNotNull(flumeMaster.getStatMan().getStatus("bar"));
    assertNotNull(flumeMaster.getStatMan().getStatus("baz"));
    sh.executeLine("exec purgeAll");
    Clock.sleep(250);

    assertNull(flumeMaster.getStatMan().getStatus("foo"));
    assertNull(flumeMaster.getStatMan().getStatus("foo"));
    assertNull(flumeMaster.getStatMan().getStatus("foo"));
  }
}
