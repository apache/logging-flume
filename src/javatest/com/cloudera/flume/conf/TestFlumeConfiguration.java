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
package com.cloudera.flume.conf;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestFlumeConfiguration {
  /**
   * This class exposes a constructor so that we can instantiate new
   * FlumeConfiguration objects.
   */
  public class TestableConfiguration extends FlumeConfiguration {
    public TestableConfiguration() {
      super(true);
    }
  }

  @Test
  public void testParseGossipServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA,hostB,hostC");
    cfg.setInt(FlumeConfiguration.MASTER_GOSSIP_PORT, 57890);

    String gossipServers = cfg.getMasterGossipServers();

    assertEquals("hostA:57890,hostB:57890,hostC:57890", gossipServers);

    assertEquals(57890, cfg.getMasterGossipPort());

    // test with spaces
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA , hostB ,     hostC");

    assertEquals("hostA:57890,hostB:57890,hostC:57890", gossipServers);

    assertEquals(57890, cfg.getMasterGossipPort());
  }

  @Test
  public void testOverrideGossipServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_GOSSIP_SERVERS,
        "hostA:57891,hostB:57892,hostC:57893");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 1);

    assertEquals(57892, cfg.getMasterGossipPort());

    // try with spaces in list
    cfg.set(FlumeConfiguration.MASTER_GOSSIP_SERVERS,
        "hostA:57891 ,  hostB:57892 , hostC:57893");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 1);

    assertEquals(57892, cfg.getMasterGossipPort());
  }

  @Test
  public void testParseZKServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA,hostB,hostC");
    cfg.setInt(FlumeConfiguration.MASTER_ZK_SERVER_PORT, 2181);
    cfg.setInt(FlumeConfiguration.MASTER_ZK_CLIENT_PORT, 3181);

    String gossipServers = cfg.getMasterZKServers();

    assertEquals("hostA:3181:2181,hostB:3181:2181,hostC:3181:2181",
        gossipServers);

    assertEquals(2181, cfg.getMasterZKServerPort());
    assertEquals(3181, cfg.getMasterZKClientPort());

    // try with arbitrary spaces
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "   hostA , hostB ,    hostC");
    assertEquals("hostA:3181:2181,hostB:3181:2181,hostC:3181:2181",
        gossipServers);

  }

  @Test
  public void testOverrideZKServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS,
        "hostA:1234:2345,hostB:1235:2346,hostC:1236:2347");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 1);

    assertEquals(1235, cfg.getMasterZKClientPort());
    assertEquals(2346, cfg.getMasterZKServerPort());

    // try with spaces
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS,
        "  hostA: 1234:2345   ,hostB: 1235:   2346 ,  hostC:1236:2347");
    assertEquals(1235, cfg.getMasterZKClientPort());
    assertEquals(2346, cfg.getMasterZKServerPort());

    // overriding settings
    cfg.setInt(FlumeConfiguration.MASTER_ZK_CLIENT_PORT, 9999);
    cfg.setInt(FlumeConfiguration.MASTER_ZK_SERVER_PORT, 9998);

    assertEquals(9999, cfg.getMasterZKClientPort());
    assertEquals(9998, cfg.getMasterZKServerPort());

  }

  @Test
  public void testParseHeartbeatServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA,hostB,hostC");
    cfg.setInt(FlumeConfiguration.MASTER_HEARTBEAT_PORT, 65432);

    String heartbeatServers = cfg.getMasterHeartbeatServers();

    assertEquals("hostA:65432,hostB:65432,hostC:65432", heartbeatServers);

    assertEquals(65432, cfg.getMasterHeartbeatPort());
  }

  @Test
  public void testParseSingleHeartbeatServer() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA.foo.com");
    cfg.setInt(FlumeConfiguration.MASTER_HEARTBEAT_PORT, 65432);

    String heartbeatServers = cfg.getMasterHeartbeatServers();

    assertEquals("hostA.foo.com:65432", heartbeatServers);

    assertEquals(65432, cfg.getMasterHeartbeatPort());
  }

  @Test
  public void testOverrideHeartbeatServers() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_HEARTBEAT_SERVERS,
        "hostA:57891,hostB:57895,hostC:57893");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 1);

    assertEquals(57895, cfg.getMasterHeartbeatPort());

    // try with spaces
    cfg.set(FlumeConfiguration.MASTER_HEARTBEAT_SERVERS,
        "hostA : 57891 ,    hostB: 57895,   hostC:57893");
    assertEquals(57895, cfg.getMasterHeartbeatPort());
  }

  @Test
  public void testMasterIsDistributed() {
    FlumeConfiguration cfg = new TestableConfiguration();
    assertEquals(false, cfg.getMasterIsDistributed());
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "hostA,hostB");
    assertEquals(true, cfg.getMasterIsDistributed());
  }

  /**
   * Master servers with ':'s should be fixed up to use default ports instead of
   */
  @Test
  public void testInvalidMasterServersFixup() {
    FlumeConfiguration cfg = new TestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_SERVERS, "foo:12345,bar:1345");
    String zksvrs = cfg.getMasterZKServers();
    assertNotSame("foo:12345:2181:3181,bar:1345:2181:3181", zksvrs);
    assertEquals("foo:2181:3181,bar:2181:3181", zksvrs);
  }
}
