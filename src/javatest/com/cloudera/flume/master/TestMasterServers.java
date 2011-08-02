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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;

/**
 * Tests the logic behind the MasterClientServer class. RPC-specific type
 * conversion is already covered in TestRPCMechanisms, so we don't test that
 * here.
*/
public class TestMasterServers {

  FlumeMaster master = null;
  File tmpdir = null;
  FlumeConfiguration cfg;

  @Before
  public void createZKDirs() throws IOException {
    cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
    tmpdir = FileUtil.mktempdir();
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_ZK_LOGDIR,
        tmpdir.getAbsolutePath());
  }

  @After
  public void stopMaster() throws IOException {
    if (master != null) {
      master.shutdown();
      master = null;
    }
    if (tmpdir != null) {
      FileUtil.rmr(tmpdir);
      tmpdir = null;
    }
  }

  @Test
  public void testMasterClientServerThrift() throws TException, IOException {
    master = new FlumeMaster(cfg);
    master.serve();
    MasterClientServer delegate = new MasterClientServer(master, cfg);

    delegate.heartbeat("foo", "phys-node", NetUtils.localhost(), 
        StatusManager.NodeState.HELLO, 0);
    delegate.acknowledge("foo");
    delegate.checkAck("foo");
    
    Map<String, NodeStatus> statuses = master.getStatMan().getNodeStatuses();
    assertEquals(1, statuses.size());
    NodeStatus s = statuses.get("foo");
    assertNotNull(s);
    assertEquals("phys-node", s.physicalNode);

    delegate.getConfig("host");
    delegate.getLogicalNodes("host");
  }

  @Test
  public void testInvalidRPCSpec() {
    cfg.set(FlumeConfiguration.MASTER_HEARBEAT_RPC, "INVALID");
    assertEquals("THRIFT", cfg.getMasterHeartbeatRPC());
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testNullMasterClient() throws IOException {
    new MasterClientServer(null, cfg);
  }

  @Test
  public void testMasterAdminServer() throws TException, IOException,
      FlumeSpecException {
    master = new FlumeMaster(cfg);
    master.serve();
    MasterAdminServer mas = new MasterAdminServer(master, cfg);

    MockClock mclk = new MockClock(0);
    Clock.setClock(mclk);

    // populate status
    MasterClientServer delegate = new MasterClientServer(master, cfg);
    master.getSpecMan().setConfig("foo", "my-test-flow", "null", "null"); // set
                                                                          // at
                                                                          // time
                                                                          // 0
    long cfgtime = Clock.unixTime();
    boolean changed1, changed2, changed3, changed4;

    mclk.forward(250);

    // make the first stamp in the "past" to force an update
    changed1 = delegate.heartbeat("foo", "foo", NetUtils.localhost(),
        StatusManager.NodeState.HELLO, cfgtime); // new
    // node
    assertTrue(changed1);
    mclk.forward(500);

    changed2 = delegate.heartbeat("foo", "foo", NetUtils.localhost(),
        StatusManager.NodeState.HELLO, cfgtime);
    assertFalse(changed2);

    master.getSpecMan().setConfig("foo", "my-test-flow", "null", "null");
    long oldcfgtime = cfgtime;
    cfgtime = Clock.unixTime();

    mclk.forward(500);

    // "check with the last version we had"

    changed3 = delegate.heartbeat("foo", "foo", NetUtils.localhost(),
        StatusManager.NodeState.HELLO, oldcfgtime);
    assertTrue(changed3);
    mclk.forward(500);

    // "ok, we did a config update on the node"

    changed4 = delegate.heartbeat("foo", "foo", NetUtils.localhost(),
        StatusManager.NodeState.HELLO, cfgtime);
    assertFalse(changed4);
    mclk.forward(500);

    mas.getNodeStatuses();

    // calls
    mas.getConfigs();
    mas.isFailure(0);
    mas.isSuccess(0);
    mas.submit(new Command("noop", new String[0]));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullMasterAdmin() {
    try {
      new MasterAdminServer(null, cfg);
    } catch (IOException e) {

    }
  }
  
  @Test
  public void testReports() throws TException, IOException {
    FlumeConfiguration.createTestableConfiguration();
    ReportManager rptMan = ReportManager.get();
    rptMan.clear();
    FlumeConfiguration.get().set(FlumeConfiguration.WEBAPPS_PATH,
        "build/webapps");
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_STORE, "memory");
    FlumeConfiguration.get().setInt(FlumeConfiguration.MASTER_HEARTBEAT_PORT, 55556);
    FlumeMaster master = new FlumeMaster(new CommandManager(),
        new ConfigManager(), new StatusManager(), new MasterAckManager(),
        FlumeConfiguration.get());
    MasterClientServer delegate = new MasterClientServer(master, cfg);
    delegate.masterRPC.serve();
    
    ReportEvent r = new ReportEvent("foo");
    r.setStringMetric("bar", "baz");
    Map<String, ReportEvent> rptMap = new HashMap<String, ReportEvent>();
    rptMap.put("test-report", r);
    delegate.putReports(rptMap);
    
    Map<String, Reportable> reportables = rptMan.getReportables();
    
    delegate.stop();
    
    assertEquals(1, reportables.size());
    assertNotNull(reportables.get("test-report"));
    assertEquals("baz", reportables.get("test-report").getReport().getStringMetric("bar"));
    
  }

}
