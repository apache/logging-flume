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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LivenessManager;
import com.cloudera.flume.agent.MasterRPC;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.util.NetUtils;

public class TestDiskFailoverAgent {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestDiskFailoverAgent.class);

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
   * This test starts a DFO agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decomissioned. As part of decomissioning, the act of closing
   * the dfo agent should not hang the node.
   */
  @Test
  public void testActiveDFOClose() throws InterruptedException {
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    final CountDownLatch done = new CountDownLatch(1);
    new Thread("ActiveDFOClose") {
      public void run() {
        LivenessManager liveMan = node.getLivenessManager();
        try {
          // update config node to something that will be interrupted.
          LOG.info("setting to invalid dfo host");
          master.getSpecMan().setConfig("node1", "flow", "asciisynth(0)",
              "agentDFOSink(\"localhost\", 12345)");
          master.getSpecMan().addLogicalNode(NetUtils.localhost(), "node1");
          liveMan.heartbeatChecks();
          Thread.sleep(10000);

          // update config node to something that will be interrupted.
          LOG.info("!!! decommissioning node on master");
          master.getSpecMan().removeLogicalNode("node1");

          // as node do heartbeat and update due to decommission
          liveMan.heartbeatChecks();
          LOG.info("!!! node should be decommissioning on node");

        } catch (Exception e) {
          LOG.info("closed caused an error out: " + e.getMessage(), e);
          // Right now it takes about 10 seconds for the dfo deco to error out.
          done.countDown();
          return; // expected fail on purpose.
        }

        LOG.info("Clean close.");
        done.countDown();
      }
    }.start();

    // false means timeout, takes about 10 seconds to shutdown.
    assertTrue("close call hung the heartbeat", done
        .await(45, TimeUnit.SECONDS));
    assertEquals(1, node.getLogicalNodeManager().getNodes().size());

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
   */
  @Test
  public void testActiveDFOCloseBadDNS() throws InterruptedException {
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    final CountDownLatch done = new CountDownLatch(1);
    new Thread() {
      public void run() {
        LivenessManager liveMan = node.getLivenessManager();
        try {
          // update config node to something that will be interrupted.
          LOG.info("setting to invalid dfo host");
          master.getSpecMan().setConfig("node1", "flow", "asciisynth(0)",
              "agentDFOSink(\"invalid\", 12345)");
          master.getSpecMan().addLogicalNode(NetUtils.localhost(), "node1");
          liveMan.heartbeatChecks();
          Thread.sleep(20000); // Takes 10s for dns to fail

          // update config node to something that will be interrupted.
          LOG.info("!!! decommissioning node on master");
          master.getSpecMan().removeLogicalNode("node1");
          liveMan.heartbeatChecks();
          LOG.info("!!! logical node should be decommissioning on node");

        } catch (Exception e) {
          LOG.error("closed caused an error out: " + e.getMessage(), e);
          // Right now it takes about 10 seconds for the dfo deco to error out.
          done.countDown();
          return; // fail
        }

        LOG.info("Clean close.");
        done.countDown();

      }
    }.start();

    // false means timeout, takes about 10 seconds to shutdown.
    assertTrue("close call hung the heartbeat", done
        .await(45, TimeUnit.SECONDS));
    assertEquals(1, node.getLogicalNodeManager().getNodes().size());

  }

  /**
   * This test starts a E2E agent that attempts to go to a port that shouldn't
   * be open. This triggers the error recovery mechanism. We then wait for 10s
   * and then simulate the node doing a heartbeat to a master which tells that
   * that node is decommissioned. As part of decommissioning, the act of closing
   * the E2E agent should not hang the node.
   */
  @Test
  public void testActiveE2EClose() throws InterruptedException {
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    final CountDownLatch done = new CountDownLatch(1);
    new Thread("TestDiskFailoverAgent") {
      public void run() {
        LivenessManager liveMan = node.getLivenessManager();
        try {
          // update config node to something that will be interrupted.
          LOG.info("setting to invalid dfo host");
          master.getSpecMan().setConfig("node1", "flow", "asciisynth(0)",
              "agentE2ESink(\"localhost\", 12345)");
          master.getSpecMan().addLogicalNode(NetUtils.localhost(), "node1");
          liveMan.heartbeatChecks();
          Thread.sleep(10000);

          // update config node to something that will be interrupted.
          LOG.info("!!! decommissioning node on master");
          master.getSpecMan().removeLogicalNode("node1");
          liveMan.heartbeatChecks();
          LOG.info("!!! node should be decommissioning on node");

        } catch (Exception e) {
          LOG.error("closed caused an error out: " + e.getMessage(), e);
          // Right now it takes about 10 seconds for the dfo deco to error out.
          done.countDown();
          return; // fail
        }

        LOG.info("Did not expect clean close!?");

      }
    }.start();

    // false means timeout, takes about 10 seconds to shutdown.
    assertTrue("close call hung the heartbeat", done
        .await(60, TimeUnit.SECONDS));

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
   */
  @Test
  public void testActiveE2ECloseBadDNS() throws InterruptedException {
    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    final CountDownLatch done = new CountDownLatch(1);
    new Thread() {
      public void run() {
        LivenessManager liveMan = node.getLivenessManager();
        try {
          // update config node to something that will be interrupted.
          LOG.info("setting to invalid dfo host");
          master.getSpecMan().setConfig("node1", "flow", "asciisynth(0)",
              "agentE2ESink(\"localhost\", 12345)");
          master.getSpecMan().addLogicalNode(NetUtils.localhost(), "node1");
          liveMan.heartbeatChecks();
          Thread.sleep(15000); // Takes 10s for dns to fail

          // update config node to something that will be interrupted.
          LOG.info("!!! decommissioning node on master");
          master.getSpecMan().removeLogicalNode("node1");
          liveMan.heartbeatChecks();
          LOG.info("!!! node should be decommissioning on node");

        } catch (Exception e) {
          LOG.error("closed caused an error out: " + e.getMessage(), e);
          // Right now it takes about 10 seconds for the dfo deco to error out.
          done.countDown();
          return; // fail
        }

        LOG.info("Did not expect clean close? ");

      }
    }.start();

    // false means timeout, takes about 10 seconds to shutdown.
    assertTrue("close call hung the heartbeat", done.await(120,
        TimeUnit.SECONDS));

  }

}
