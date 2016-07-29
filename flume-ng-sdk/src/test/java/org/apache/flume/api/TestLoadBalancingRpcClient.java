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

import junit.framework.Assert;
import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils.LoadBalancedAvroHandler;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class TestLoadBalancingRpcClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestLoadBalancingRpcClient.class);

  @Test(expected = FlumeException.class)
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
    Server s1 = null;
    Server s2 = null;
    RpcClient c = null;
    try {
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

  // This will fail without FLUME-1823
  @Test(expected = EventDeliveryException.class)
  public void testTwoHostFailoverThrowAfterClose() throws Exception {
    Server s1 = null;
    Server s2 = null;
    RpcClient c = null;
    try {
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
      if (c != null) c.close();
      c.append(getEvent(3));
      Assert.fail();
    } finally {
      if (s1 != null) s1.close();
      if (s2 != null) s2.close();
    }
  }

  /**
   * Ensure that we can tolerate a host that is completely down.
   *
   * @throws Exception
   */
  @Test
  public void testTwoHostsOneDead() throws Exception {
    LOGGER.info("Running testTwoHostsOneDead...");
    Server s1 = null;
    RpcClient c1 = null;
    RpcClient c2 = null;
    try {
      LoadBalancedAvroHandler h1 = new LoadBalancedAvroHandler();
      s1 = RpcTestUtils.startServer(h1);
      // do not create a 2nd server (assume it's "down")

      Properties p = new Properties();
      p.put("hosts", "h1 h2");
      p.put("client.type", "default_loadbalance");
      p.put("hosts.h1", "127.0.0.1:" + 0); // port 0 should always be closed
      p.put("hosts.h2", "127.0.0.1:" + s1.getPort());

      // test batch API
      c1 = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c1 instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 10; i++) {
        c1.appendBatch(getBatchedEvent(i));
      }
      Assert.assertEquals(10, h1.getAppendBatchCount());

      // test non-batch API
      c2 = RpcClientFactory.getInstance(p);
      Assert.assertTrue(c2 instanceof LoadBalancingRpcClient);

      for (int i = 0; i < 10; i++) {
        c2.append(getEvent(i));
      }
      Assert.assertEquals(10, h1.getAppendCount());


    } finally {
      if (s1 != null) s1.close();
      if (c1 != null) c1.close();
      if (c2 != null) c2.close();
    }
  }

  @Test
  public void testTwoHostFailoverBatch() throws Exception {
    Server s1 = null;
    Server s2 = null;
    RpcClient c = null;
    try {
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
    Server s1 = null;
    Server s2 = null;
    RpcClient c = null;
    try {
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
    Server s1 = null;
    Server s2 = null;
    RpcClient c = null;
    try {
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
    try {
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i < NUM_HOSTS; i++) {
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
      for (int i = 0; i < NUM_HOSTS; i++) {
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
    try {
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i < NUM_HOSTS; i++) {
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
      for (int i = 0; i < NUM_HOSTS; i++) {
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
    try {
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i < NUM_HOSTS; i++) {
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
      for (int i = 0; i < NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  @Test
  public void testLbClientTenHostRoundRobinDistributionBatch() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    Server[] s = new Server[NUM_HOSTS];
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    RpcClient c = null;
    try {
      Properties p = new Properties();
      StringBuilder hostList = new StringBuilder("");
      for (int i = 0; i < NUM_HOSTS; i++) {
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
      for (int i = 0; i < NUM_HOSTS; i++) {
        if (s[i] != null) s[i].close();
      }
    }
  }

  @Test
  public void testRandomBackoff() throws Exception {
    Properties p = new Properties();
    List<LoadBalancedAvroHandler> hosts =
        new ArrayList<LoadBalancedAvroHandler>();
    List<Server> servers = new ArrayList<Server>();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i < 3; i++) {
      LoadBalancedAvroHandler s = new LoadBalancedAvroHandler();
      hosts.add(s);
      Server srv = RpcTestUtils.startServer(s);
      servers.add(srv);
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:" + srv.getPort());
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "random");
    p.put("backoff", "true");
    hosts.get(0).setFailed();
    hosts.get(2).setFailed();

    RpcClient c = RpcClientFactory.getInstance(p);
    Assert.assertTrue(c instanceof LoadBalancingRpcClient);

    // TODO: there is a remote possibility that s0 or s2
    // never get hit by the random assignment
    // and thus not backoffed, causing the test to fail
    for (int i = 0; i < 50; i++) {
      // a well behaved runner would always check the return.
      c.append(EventBuilder.withBody(("test" + String.valueOf(i)).getBytes()));
    }
    Assert.assertEquals(50, hosts.get(1).getAppendCount());
    Assert.assertEquals(0, hosts.get(0).getAppendCount());
    Assert.assertEquals(0, hosts.get(2).getAppendCount());
    hosts.get(0).setOK();
    hosts.get(1).setFailed(); // s0 should still be backed off
    try {
      c.append(EventBuilder.withBody("shouldfail".getBytes()));
      // nothing should be able to process right now
      Assert.fail("Expected EventDeliveryException");
    } catch (EventDeliveryException e) {
      // this is expected
    }
    Thread.sleep(2500); // wait for s0 to no longer be backed off

    for (int i = 0; i < 50; i++) {
      // a well behaved runner would always check the return.
      c.append(EventBuilder.withBody(("test" + String.valueOf(i)).getBytes()));
    }
    Assert.assertEquals(50, hosts.get(0).getAppendCount());
    Assert.assertEquals(50, hosts.get(1).getAppendCount());
    Assert.assertEquals(0, hosts.get(2).getAppendCount());
  }

  @Test
  public void testRoundRobinBackoffInitialFailure() throws EventDeliveryException {
    Properties p = new Properties();
    List<LoadBalancedAvroHandler> hosts =
        new ArrayList<LoadBalancedAvroHandler>();
    List<Server> servers = new ArrayList<Server>();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i < 3; i++) {
      LoadBalancedAvroHandler s = new LoadBalancedAvroHandler();
      hosts.add(s);
      Server srv = RpcTestUtils.startServer(s);
      servers.add(srv);
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:" + srv.getPort());
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");
    p.put("backoff", "true");

    RpcClient c = RpcClientFactory.getInstance(p);
    Assert.assertTrue(c instanceof LoadBalancingRpcClient);

    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    hosts.get(1).setFailed();
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    hosts.get(1).setOK();
    //This time the iterators will never have "1".
    //So clients get in the order: 1 - 3 - 1
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }

    Assert.assertEquals(1 + 2 + 1, hosts.get(0).getAppendCount());
    Assert.assertEquals(1, hosts.get(1).getAppendCount());
    Assert.assertEquals(1 + 1 + 2, hosts.get(2).getAppendCount());
  }

  @Test
  public void testRoundRobinBackoffIncreasingBackoffs() throws Exception {
    Properties p = new Properties();
    List<LoadBalancedAvroHandler> hosts =
        new ArrayList<LoadBalancedAvroHandler>();
    List<Server> servers = new ArrayList<Server>();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i < 3; i++) {
      LoadBalancedAvroHandler s = new LoadBalancedAvroHandler();
      hosts.add(s);
      if (i == 1) {
        s.setFailed();
      }
      Server srv = RpcTestUtils.startServer(s);
      servers.add(srv);
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:" + srv.getPort());
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");
    p.put("backoff", "true");

    RpcClient c = RpcClientFactory.getInstance(p);
    Assert.assertTrue(c instanceof LoadBalancingRpcClient);

    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    Assert.assertEquals(0, hosts.get(1).getAppendCount());
    Thread.sleep(2100);
    // this should let the sink come out of backoff and get backed off  for a longer time
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    Assert.assertEquals(0, hosts.get(1).getAppendCount());
    hosts.get(1).setOK();
    Thread.sleep(2100);
    // this time it shouldn't come out of backoff yet as the timeout isn't over
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));

    }
    Assert.assertEquals(0, hosts.get(1).getAppendCount());
    // after this s2 should be receiving events again
    Thread.sleep(2500);
    int numEvents = 60;
    for (int i = 0; i < numEvents; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }

    Assert.assertEquals(2 + 2 + 1 + (numEvents / 3), hosts.get(0).getAppendCount());
    Assert.assertEquals((numEvents / 3), hosts.get(1).getAppendCount());
    Assert.assertEquals(1 + 1 + 2 + (numEvents / 3), hosts.get(2).getAppendCount());
  }

  @Test
  public void testRoundRobinBackoffFailureRecovery()
      throws EventDeliveryException, InterruptedException {
    Properties p = new Properties();
    List<LoadBalancedAvroHandler> hosts =
        new ArrayList<LoadBalancedAvroHandler>();
    List<Server> servers = new ArrayList<Server>();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i < 3; i++) {
      LoadBalancedAvroHandler s = new LoadBalancedAvroHandler();
      hosts.add(s);
      if (i == 1) {
        s.setFailed();
      }
      Server srv = RpcTestUtils.startServer(s);
      servers.add(srv);
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:" + srv.getPort());
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");
    p.put("backoff", "true");

    RpcClient c = RpcClientFactory.getInstance(p);
    Assert.assertTrue(c instanceof LoadBalancingRpcClient);


    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("recovery test".getBytes()));
    }
    hosts.get(1).setOK();
    Thread.sleep(3000);
    int numEvents = 60;

    for (int i = 0; i < numEvents; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }

    Assert.assertEquals(2 + (numEvents / 3), hosts.get(0).getAppendCount());
    Assert.assertEquals(0 + (numEvents / 3), hosts.get(1).getAppendCount());
    Assert.assertEquals(1 + (numEvents / 3), hosts.get(2).getAppendCount());
  }

  private List<Event> getBatchedEvent(int index) {
    List<Event> result = new ArrayList<Event>();
    result.add(getEvent(index));
    return result;
  }

  private Event getEvent(int index) {
    return EventBuilder.withBody(("event: " + index).getBytes());
  }

}
