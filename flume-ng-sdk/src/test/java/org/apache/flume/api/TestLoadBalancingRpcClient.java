/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.api;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.Assert;

import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils.LoadBalancedAvroHandler;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLoadBalancingRpcClient {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestLoadBalancingRpcClient.class);


  @Test(expected=FlumeException.class)
  public void testCreatingLbClientSingleHost() {
    Server server1 = null;
    RpcClient c = null;
    try {
      server1 = RpcTestUtils.startServer(new OKAvroHandler());
      Properties p = new Properties();
      p.put("host1", "127.0.0.1:" + server1.getPort());
      p.put("hosts", "host1");
      p.put("client.type", "default_loadbalance");
      RpcClientFactory.getInstance(p);
    } finally {
      if (server1 != null) server1.close();
      if (c != null) c.close();
    }
  }

  @Test
  public void testTwoHostFailover() throws Exception {
    Server s1 = null, s2 = null;
    RpcClient c = null;
    try{
      LoadBalancedAvroHandler h1 = new LoadBalancedAvroHandler();
      LoadBalancedAvroHandler h2 = new LoadBalancedAvroHandler();

      s1 = RpcTestUtils.startServer(h1);
      s2 = RpcTestUtils.startServer(h2);

      Properties p = new Properties();
      p.put("hosts", "h1 h2");
      p.put("client.type", "default_loadbalance");
      p.put("hosts.h1", "127.0.0.1:" + s1.getPort());
      p.put("hosts.h2", "127.0.0.1:" + s2.getPort());

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 100; i++) {
        if (i == 20) {
          h2.setFailed();
        } else if (i == 40) {
          h2.setOK();
        }
        c.append(getEvent(i));
      }

      Assert.assertEquals(60, h1.getAppendCount());
      Assert.assertEquals(40, h2.getAppendCount());
    } finally {
      if (s1 != null) s1.close();
      if (s2 != null) s2.close();
      if (c != null) c.close();
    }
  }

  @Test
  public void testTwoHostFailoverBatch() throws Exception {
    Server s1 = null, s2 = null;
    RpcClient c = null;
    try{
      LoadBalancedAvroHandler h1 = new LoadBalancedAvroHandler();
      LoadBalancedAvroHandler h2 = new LoadBalancedAvroHandler();

      s1 = RpcTestUtils.startServer(h1);
      s2 = RpcTestUtils.startServer(h2);

      Properties p = new Properties();
      p.put("hosts", "h1 h2");
      p.put("client.type", "default_loadbalance");
      p.put("hosts.h1", "127.0.0.1:" + s1.getPort());
      p.put("hosts.h2", "127.0.0.1:" + s2.getPort());

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 100; i++) {
        if (i == 20) {
          h2.setFailed();
        } else if (i == 40) {
          h2.setOK();
        }

        c.appendBatch(getBatchedEvent(i));
      }

      Assert.assertEquals(60, h1.getAppendBatchCount());
      Assert.assertEquals(40, h2.getAppendBatchCount());
    } finally {
      if (s1 != null) s1.close();
      if (s2 != null) s2.close();
      if (c != null) c.close();
    }
  }

  @Test
  public void testLbDefaultClientTwoHosts() throws Exception {
    Server s1 = null, s2 = null;
    RpcClient c = null;
    try{
      LoadBalancedAvroHandler h1 = new LoadBalancedAvroHandler();
      LoadBalancedAvroHandler h2 = new LoadBalancedAvroHandler();

      s1 = RpcTestUtils.startServer(h1);
      s2 = RpcTestUtils.startServer(h2);

      Properties p = new Properties();
      p.put("hosts", "h1 h2");
      p.put("client.type", "default_loadbalance");
      p.put("hosts.h1", "127.0.0.1:" + s1.getPort());
      p.put("hosts.h2", "127.0.0.1:" + s2.getPort());

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 100; i++) {
        c.append(getEvent(i));
      }

      Assert.assertEquals(50, h1.getAppendCount());
      Assert.assertEquals(50, h2.getAppendCount());
    } finally {
      if (s1 != null) s1.close();
      if (s2 != null) s2.close();
      if (c != null) c.close();
    }
  }

  @Test
  public void testLbDefaultClientTwoHostsBatch() throws Exception {
    Server s1 = null, s2 = null;
    RpcClient c = null;
    try{
      LoadBalancedAvroHandler h1 = new LoadBalancedAvroHandler();
      LoadBalancedAvroHandler h2 = new LoadBalancedAvroHandler();

      s1 = RpcTestUtils.startServer(h1);
      s2 = RpcTestUtils.startServer(h2);

      Properties p = new Properties();
      p.put("hosts", "h1 h2");
      p.put("client.type", "default_loadbalance");
      p.put("hosts.h1", "127.0.0.1:" + s1.getPort());
      p.put("hosts.h2", "127.0.0.1:" + s2.getPort());

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 100; i++) {
        c.appendBatch(getBatchedEvent(i));
      }

      Assert.assertEquals(50, h1.getAppendBatchCount());
      Assert.assertEquals(50, h2.getAppendBatchCount());
    } finally {
      if (s1 != null) s1.close();
      if (s2 != null) s2.close();
      if (c != null) c.close();
    }
  }

  @Test
  public void testLbClientTenHostRandomDistribution() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    Server[] s = new Server[NUM_HOSTS];
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    RpcClient c = null;
    try{
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i<NUM_HOSTS; i++) {
        h[i] = new LoadBalancedAvroHandler();
        s[i] = RpcTestUtils.startServer(h[i]);
        String name = "h" + i;
        p.put("hosts." + name, "127.0.0.1:" + s[i].getPort());
        hostList.append(name).append(" ");
      }

      p.put("hosts", hostList.toString().trim());
      p.put("client.type", "default_loadbalance");
      p.put("host-selector", "random");

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < NUM_EVENTS; i++) {
        c.append(getEvent(i));
      }

      Set<Integer> counts = new HashSet<Integer>();
      int total = 0;
      for (LoadBalancedAvroHandler handler : h) {
        total += handler.getAppendCount();
        counts.add(handler.getAppendCount());
      }

      Assert.assertTrue("Very unusual distribution", counts.size() > 2);
      Assert.assertTrue("Missing events", total == NUM_EVENTS);
    } finally {
      for (int i = 0; i<NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  @Test
  public void testLbClientTenHostRandomDistributionBatch() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    Server[] s = new Server[NUM_HOSTS];
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    RpcClient c = null;
    try{
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i<NUM_HOSTS; i++) {
        h[i] = new LoadBalancedAvroHandler();
        s[i] = RpcTestUtils.startServer(h[i]);
        String name = "h" + i;
        p.put("hosts." + name, "127.0.0.1:" + s[i].getPort());
        hostList.append(name).append(" ");
      }

      p.put("hosts", hostList.toString().trim());
      p.put("client.type", "default_loadbalance");
      p.put("host-selector", "random");

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < NUM_EVENTS; i++) {
        c.appendBatch(getBatchedEvent(i));
      }

      Set<Integer> counts = new HashSet<Integer>();
      int total = 0;
      for (LoadBalancedAvroHandler handler : h) {
        total += handler.getAppendBatchCount();
        counts.add(handler.getAppendBatchCount());
      }

      Assert.assertTrue("Very unusual distribution", counts.size() > 2);
      Assert.assertTrue("Missing events", total == NUM_EVENTS);
    } finally {
      for (int i = 0; i<NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  @Test
  public void testLbClientTenHostRoundRobinDistribution() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    Server[] s = new Server[NUM_HOSTS];
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    RpcClient c = null;
    try{
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i<NUM_HOSTS; i++) {
        h[i] = new LoadBalancedAvroHandler();
        s[i] = RpcTestUtils.startServer(h[i]);
        String name = "h" + i;
        p.put("hosts." + name, "127.0.0.1:" + s[i].getPort());
        hostList.append(name).append(" ");
      }

      p.put("hosts", hostList.toString().trim());
      p.put("client.type", "default_loadbalance");
      p.put("host-selector", "round_robin");

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < NUM_EVENTS; i++) {
        c.append(getEvent(i));
      }

      Set<Integer> counts = new HashSet<Integer>();
      int total = 0;
      for (LoadBalancedAvroHandler handler : h) {
        total += handler.getAppendCount();
        counts.add(handler.getAppendCount());
      }

      Assert.assertTrue("Very unusual distribution", counts.size() == 1);
      Assert.assertTrue("Missing events", total == NUM_EVENTS);
    } finally {
      for (int i = 0; i<NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  @Test
  public void testLbClientTenHostRoundRobinDistributionBatch() throws Exception
  {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    Server[] s = new Server[NUM_HOSTS];
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    RpcClient c = null;
    try{
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i<NUM_HOSTS; i++) {
        h[i] = new LoadBalancedAvroHandler();
        s[i] = RpcTestUtils.startServer(h[i]);
        String name = "h" + i;
        p.put("hosts." + name, "127.0.0.1:" + s[i].getPort());
        hostList.append(name).append(" ");
      }

      p.put("hosts", hostList.toString().trim());
      p.put("client.type", "default_loadbalance");
      p.put("host-selector", "round_robin");

      c = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c instanceof LoadBalancingRpcClient);

      for (int i = 0; i < NUM_EVENTS; i++) {
        c.appendBatch(getBatchedEvent(i));
      }

      Set<Integer> counts = new HashSet<Integer>();
      int total = 0;
      for (LoadBalancedAvroHandler handler : h) {
        total += handler.getAppendBatchCount();
        counts.add(handler.getAppendBatchCount());
      }

      Assert.assertTrue("Very unusual distribution", counts.size() == 1);
      Assert.assertTrue("Missing events", total == NUM_EVENTS);
    } finally {
      for (int i = 0; i<NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  private List<Event> getBatchedEvent(int index) {
    List<Event> result = new ArrayList<Event>();
    result.add(EventBuilder.withBody(("event: " + index).getBytes()));
    return result;
  }

  private Event getEvent(int index) {
    return EventBuilder.withBody(("event: " + index).getBytes());
  }

}
