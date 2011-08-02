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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;

/**
 * These test the status manager. It simulates sending heartbeats and does a
 * timeout check
 */
public class TestStatusManager {

  public static Logger LOG = Logger.getLogger(TestStatusManager.class);

  @Test
  public void testStatusManagerHeartbeats() throws IOException {
    StatusManager stats = new StatusManager();

    // foo is in state HELLO and has configuration numbered 0
    long prev = Clock.unixTime();
    boolean needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.HELLO, 0);
    LOG.info(stats.getNodeStatuses());
    assertTrue(needsRefresh);

    StatusManager.NodeStatus ns = stats.getNodeStatuses().get("foo");
    assertEquals(0, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.HELLO, ns.state);
    prev = ns.lastseen;

    needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.IDLE, 0);
    LOG.info(stats.getNodeStatuses());
    assertFalse(needsRefresh); // no new data flow version
    assertEquals(0, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.IDLE, ns.state);
    prev = ns.lastseen;

    needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.ACTIVE, 10);
    LOG.info(stats.getNodeStatuses());
    assertFalse(needsRefresh); // node has updated version number, but master
    // has not
    assertEquals(10, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.ACTIVE, ns.state);
    prev = ns.lastseen;

    // same message, no refresh
    needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.ACTIVE, 10);
    LOG.info(stats.getNodeStatuses());
    assertFalse(needsRefresh); // this is wierd, is this right?
    assertEquals(10, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.ACTIVE, ns.state);
    prev = ns.lastseen;

    // regress to an older version,
    needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.ACTIVE, 5);
    LOG.info(stats.getNodeStatuses());
    assertFalse(needsRefresh);
    assertEquals(5, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.ACTIVE, ns.state);
    prev = ns.lastseen;

    LOG.info(stats.getReport());
    LOG.info(stats.getName());
  }

  @Test
  public void testCheckup() throws IOException {
    MockClock clk = new MockClock(0);
    Clock.setClock(clk);
    StatusManager stats = new StatusManager();

    // foo is in state HELLO and has configuration numbered 0
    long prev = Clock.unixTime();
    boolean needsRefresh = stats.updateHeartbeatStatus(NetUtils.localhost(),
        "physnode", "foo", NodeState.HELLO, 0);
    LOG.info(stats.getNodeStatuses());
    assertTrue(needsRefresh);

    // move forward in time, but not far enough to trigger being lost
    clk.forward(FlumeConfiguration.get().getConfigHeartbeatPeriod() * 5);
    stats.checkup();
    StatusManager.NodeStatus ns = stats.getNodeStatuses().get("foo");
    assertEquals(0, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.HELLO, ns.state);
    prev = ns.lastseen;

    clk.forward(FlumeConfiguration.get().getConfigHeartbeatPeriod() * 20);
    stats.checkup();
    ns = stats.getNodeStatuses().get("foo");
    assertEquals(0, ns.version);
    assertTrue(prev <= ns.lastseen);
    assertEquals(NodeState.LOST, ns.state);
    prev = ns.lastseen;

    LOG.info(ns.toString());
    LOG.info(stats.getStatus("foo").toString());
  }

}
