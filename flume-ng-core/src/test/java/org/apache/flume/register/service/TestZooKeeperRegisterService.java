/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.register.service;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.flume.Context;
import org.apache.flume.register.service.RegisterService;
import org.apache.flume.register.service.ZooKeeperRegisterService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZooKeeperRegisterService {

  private TestingServer zkServer;
  private CuratorFramework client;
  
  @Before
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    client = CuratorFrameworkFactory
        .newClient("localhost:" + zkServer.getPort(),
            new ExponentialBackoffRetry(1000, 3));
    client.start();
  }
  
  @Test
  public void testZooKeeperRegistration() throws Exception {
    Context ctx = new Context();
    String[] hostPort = zkServer.getConnectString().split(":");
    ctx.put(ZooKeeperRegisterService.ZK_HOST_KEY, hostPort[0]);
    ctx.put(ZooKeeperRegisterService.ZK_PORT_KEY, hostPort[1]);
    ctx.put(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY, "/testzk");
    ctx.put(ZooKeeperRegisterService.ZK_ENDPOINT_PORT_KEY, "1463");
    ctx.put(ZooKeeperRegisterService.TIER_KEY, "tier1 tier2");
    
    RegisterService registerService = new ZooKeeperRegisterService();
    registerService.configure(ctx);
    registerService.start();
    
    GetChildrenBuilder builder = client.getChildren();
    List<String> paths = builder.forPath(
        ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY));
    assertEquals(paths.size(), 2);
    
    for (String str : paths) {
      List<String> ephemeralNodes = builder.forPath(
          ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY) + "/" + str);
      assertEquals(ephemeralNodes.size(), 1);
      String expectedZNodeName = InetAddress.getLocalHost().getHostName() +
          ":" + ctx.getString(ZooKeeperRegisterService.ZK_ENDPOINT_PORT_KEY);
      assertEquals(expectedZNodeName, ephemeralNodes.get(0));
    }

    registerService.stop();
    // Add new tier and check
    ctx.put(ZooKeeperRegisterService.TIER_KEY, "tier1 tier2 tier3");
    registerService.configure(ctx);
    registerService.start();

    builder = client.getChildren();
    paths = builder.forPath(
        ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY));
    assertEquals(paths.size(), 3);
    for (String str : paths) {
      List<String> ephemeralNodes = builder.forPath(
          ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY) + "/" + str);
      assertEquals(ephemeralNodes.size(), 1);
      String expectedZNodeName = InetAddress.getLocalHost().getHostName() +
          ":" + ctx.getString(ZooKeeperRegisterService.ZK_ENDPOINT_PORT_KEY);
      assertEquals(expectedZNodeName, ephemeralNodes.get(0));
    }

    registerService.stop();
    // Delete tier and check
    ctx.put(ZooKeeperRegisterService.TIER_KEY, "tier1");
    registerService.configure(ctx);
    registerService.start();

    builder = client.getChildren();
    paths = builder.forPath(
        ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY));
    assertEquals(paths.size(), 3);
    for (String str : paths) {
      List<String> ephemeralNodes = builder.forPath(
          ctx.getString(ZooKeeperRegisterService.ZK_PATH_PREFIX_KEY) + "/" + str);
      if (str.contains("tier1")) {
        assertEquals(ephemeralNodes.size(), 1);
        String expectedZNodeName = InetAddress.getLocalHost().getHostName() +
            ":" + ctx.getString(ZooKeeperRegisterService.ZK_ENDPOINT_PORT_KEY);
        assertEquals(expectedZNodeName, ephemeralNodes.get(0));
      } else {
        assertEquals(ephemeralNodes.size(), 0);
      }
    }
  }
  
  @After
  public void tearDown() throws Exception {
    client.close();
    zkServer.stop();
  }
  
  
}
