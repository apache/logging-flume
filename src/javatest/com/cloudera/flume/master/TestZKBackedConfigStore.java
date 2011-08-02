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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.Pair;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class TestZKBackedConfigStore {
  protected static Logger LOG = Logger.getLogger(TestZKBackedConfigStore.class);

  /**
   * Test that set and get work correctly, and that recovery after restart works
   * correctly.
   */
  @Test
  public void testZKBackedConfigStore() throws IOException,
      InterruptedException {
    for (int i = 0; i < 10; ++i) {
      LOG.info("Opening ZK, attempt " + i);
      File tmp = FileUtil.mktempdir();
      FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
      cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
      cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
      ZooKeeperService.getAndInit(cfg);

      ZooKeeperConfigStore store = new ZooKeeperConfigStore();
      store.init();
      ConfigManager manager = new ConfigManager(store);
      manager.setConfig("foo", "my-test-flow", "null", "console");
      FlumeConfigData data = manager.getConfig("foo");
      assertEquals(data.getSinkConfig(), "console");
      assertEquals(data.getSourceConfig(), "null");
      store.shutdown();

      store = new ZooKeeperConfigStore();
      store.init();
      manager = new ConfigManager(store);
      data = manager.getConfig("foo");
      assertEquals(data.getSinkConfig(), "console");
      assertEquals(data.getSourceConfig(), "null");

      Map<String, FlumeConfigData> cfgs = new HashMap<String, FlumeConfigData>();
      String defaultFlowName = cfg.getDefaultFlowName();
      cfgs.put("bulk1", new FlumeConfigData(0, "s1", "sk1",
          LogicalNode.VERSION_INFIMUM, LogicalNode.VERSION_INFIMUM,
          "my-test-flow"));
      cfgs.put("bulk2", new FlumeConfigData(0, "s2", "sk2",
          LogicalNode.VERSION_INFIMUM, LogicalNode.VERSION_INFIMUM,
          defaultFlowName));
      store.bulkSetConfig(cfgs);

      data = manager.getConfig("bulk1");
      assertEquals(data.getSinkConfig(), "sk1");
      assertEquals(data.getSourceConfig(), "s1");
      assertEquals(data.getFlowID(), "my-test-flow");

      data = manager.getConfig("bulk2");
      assertEquals(data.getSinkConfig(), "sk2");
      assertEquals(data.getSourceConfig(), "s2");

      cfgs.put("bulk1", new FlumeConfigData(0, "s3", "sk3",
          LogicalNode.VERSION_INFIMUM, LogicalNode.VERSION_INFIMUM,
          defaultFlowName));
      cfgs.remove("bulk2");
      store.bulkSetConfig(cfgs);

      // Check that unchanged configs persist
      data = manager.getConfig("bulk2");
      assertEquals(data.getSinkConfig(), "sk2");
      assertEquals(data.getSourceConfig(), "s2");
      assertEquals(data.getFlowID(), defaultFlowName);
      store.shutdown();

      ZooKeeperService.get().shutdown();
      FileUtil.rmr(tmp);
    }
  }

  /**
   * Test that set and get work correctly, and that recovery after restart works
   * correctly.
   *
   * TODO add mechanism to close a ZooKeeperConfigStore to release resources.
   * (picking different ports right now to make test pass)
   */
  @Test
  public void testZKBackedConfigStoreNodes() throws IOException,
      InterruptedException {
    File tmp = FileUtil.mktempdir();
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    ZooKeeperService.getAndInit(cfg);

    ZooKeeperConfigStore store = new ZooKeeperConfigStore();
    store.init();

    ConfigManager manager = new ConfigManager(store);
    manager.addLogicalNode("physical", "logical1");
    manager.addLogicalNode("physical", "logical2");
    manager.addLogicalNode("physical", "logical3");
    manager.addLogicalNode("p2", "l2");
    manager.addLogicalNode("p3", "l3");

    List<String> lns = manager.getLogicalNode("physical");
    assertTrue(lns.contains("logical1"));
    assertTrue(lns.contains("logical2"));
    assertTrue(lns.contains("logical3"));

    assertTrue(manager.getLogicalNode("p2").contains("l2"));
    assertTrue(manager.getLogicalNode("p3").contains("l3"));
    store.shutdown();

    store = new ZooKeeperConfigStore();
    store.init();
    manager = new ConfigManager(store);

    lns = manager.getLogicalNode("physical");
    assertTrue(lns.contains("logical1"));
    assertTrue(lns.contains("logical2"));
    assertTrue(lns.contains("logical3"));

    assertTrue(manager.getLogicalNode("p2").contains("l2"));
    assertTrue(manager.getLogicalNode("p3").contains("l3"));
    store.shutdown();

    ZooKeeperService.get().shutdown();

    FileUtil.rmr(tmp);
  }

  /**
   * Test that watches are fired correctly for logical nodes
   */
  @Test
  public void testZBCSLogicalWatches() throws IOException, InterruptedException {
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    File tmp = FileUtil.mktempdir();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.setBoolean(FlumeConfiguration.MASTER_ZK_USE_EXTERNAL, false);
    ZooKeeperService.getAndInit(cfg);

    ZooKeeperConfigStore store = new ZooKeeperConfigStore();
    store.init();
    ZooKeeperConfigStore store2 = new ZooKeeperConfigStore();
    store2.init();
    ConfigManager manager1 = new ConfigManager(store);
    ConfigManager manager2 = new ConfigManager(store2);
    manager1.addLogicalNode("logical-watch", "logical1");

    // There is no convenient way to avoid this sleep
    Thread.sleep(2000);

    // Check that the watch has happened and that the new value
    // will be correctly read
    assertEquals("logical1", manager2.getLogicalNode("logical-watch").get(0));
    store.shutdown();
    store2.shutdown();
    ZooKeeperService.get().shutdown();
    FileUtil.rmr(tmp);
  }

  /**
   * Test disconnection
   */
  @Test
  public void testZBCSLoseZKCnxn() throws IOException, InterruptedException {
    File tmp = FileUtil.mktempdir();
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    ZooKeeperService.getAndInit(cfg);

    ZooKeeperConfigStore store = new ZooKeeperConfigStore();
    store.init();
    store.setConfig("foo", "my-test-flow", "fab", "fat");

    ZooKeeperService.get().shutdown();
    Thread.sleep(30 * 1000);
    IOException ex = null;

    try {
      store.setConfig("foo", cfg.getDefaultFlowName(), "bar", "baz");
    } catch (IOException e) {
      ex = e;
    }
    assertNotNull("Expected IOException not thrown - still connected to ZK?",
        ex);
    store.shutdown();
    FileUtil.rmr(tmp);
  }

  final CountDownLatch latch = new CountDownLatch(3);

  /**
   * Initialises a single server of a ZK ensemble. Must be threaded so that all
   * servers can come up at once.
   */
  protected class ZKThread extends Thread {
    protected final int serverid;
    final File tmp;
    final FlumeConfiguration cfg = FlumeConfiguration
        .createTestableConfiguration();
    final ZooKeeperService zkService = new ZooKeeperService();

    public ZKThread(int serverid) throws IOException {
      super("ZKThread-" + serverid);
      this.serverid = serverid;
      tmp = FileUtil.mktempdir();
    }

    @Override
    public void run() {
      cfg
          .set(FlumeConfiguration.MASTER_ZK_SERVERS,
              "localhost:2181:3181:4181,localhost:2182:3182:4182,localhost:2183:3183:4183");
      cfg.set(FlumeConfiguration.MASTER_SERVERS,
          "localhost,localhost,localhost");
      cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());

      cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, serverid);
      try {
        zkService.init(cfg);
      } catch (Exception e) {
        LOG.error("Exception when starting ZK " + serverid, e);

        // Not counting down the latch will cause the calling test to timeout
        return;
      }
      latch.countDown();
    }

    public ZooKeeperService getService() {
      return zkService;
    }

    public void shutdown() throws IOException {
      zkService.shutdown();
      FileUtil.rmr(tmp);
    }
  }

  /**
   * Test good behaviour when servers fail.
   */
  @Test
  public void testEnsembleFailure() throws IOException, InterruptedException {
    ZKThread zk1 = new ZKThread(0);
    ZKThread zk2 = new ZKThread(1);
    ZKThread zk3 = new ZKThread(2);

    zk1.start();
    zk2.start();
    zk3.start();

    if (!latch.await(10, TimeUnit.SECONDS)) {
      fail("ZooKeeper did not come up!");
    }

    ZooKeeperConfigStore store = new ZooKeeperConfigStore(zk1.getService());
    store.init();

    String defaultFlowName = FlumeConfiguration.get().getDefaultFlowName();
    store.setConfig("foo", defaultFlowName, "null", "baz");

    zk1.shutdown();

    store.setConfig("foo2", defaultFlowName, "baz", "bar");

    zk2.shutdown();
    zk3.shutdown();
    store.shutdown();
  }

  /**
   * Test to make sure unmapping logical nodes from physical nodes and survives
   * a zk restart.
   */
  @Test
  public void testUnmapAllNodes() throws IOException, InterruptedException {

    File tmp = FileUtil.mktempdir();
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 0);
    ZooKeeperService.getAndInit(cfg);
    ZooKeeperConfigStore store = new ZooKeeperConfigStore();
    store.init();

    ConfigManager manager = new ConfigManager(store);
    manager.addLogicalNode("physical", "logical1");
    manager.addLogicalNode("physical", "logical2");
    manager.addLogicalNode("physical", "logical3");
    manager.addLogicalNode("p2", "l2");
    manager.addLogicalNode("p3", "l3");

    manager.unmapAllLogicalNodes();

    List<String> lns = manager.getLogicalNode("physical");
    assertFalse(lns.contains("logical1"));
    assertFalse(lns.contains("logical2"));
    assertFalse(lns.contains("logical3"));

    assertFalse(manager.getLogicalNode("p2").contains("l2"));
    assertFalse(manager.getLogicalNode("p3").contains("l3"));

    store.shutdown();
    store = new ZooKeeperConfigStore();
    store.init();
    manager = new ConfigManager(store);

    lns = manager.getLogicalNode("physical");
    assertFalse(lns.contains("logical1"));
    assertFalse(lns.contains("logical2"));
    assertFalse(lns.contains("logical3"));

    assertFalse(manager.getLogicalNode("p2").contains("l2"));
    assertFalse(manager.getLogicalNode("p3").contains("l3"));
    store.shutdown();
    ZooKeeperService.get().shutdown();
    FileUtil.rmr(tmp);
  }

  /**
   * Test that the version is correctly incremented
   */
  @Test
  public void testVersionIncrement() throws IOException, InterruptedException,
      KeeperException {
    File tmp = FileUtil.mktempdir();
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 0);

    ZooKeeperService zk = new ZooKeeperService();
    zk.init(cfg);

    ZooKeeperConfigStore store = new ZooKeeperConfigStore(zk);
    store.init();
    String defaultFlowName = cfg.getDefaultFlowName();
    store.setConfig("foo", defaultFlowName, "null", "baz");
    store.setConfig("foo2", defaultFlowName, "null", "baz");
    store.setConfig("foo3", defaultFlowName, "null", "baz");

    ZKClient client = zk.createClient();
    client.init();
    List<String> children = client.getChildren(ZooKeeperConfigStore.CFGS_PATH,
        false);
    assertEquals("Expected 3 configs", 3, children.size());

    // Note children not necessarily returned in creation order
    Collections.sort(children);

    assertEquals("Expected config to be numbered 0", 0L, ZKClient
        .extractSuffix("cfg-", children.get(0)));
    assertEquals("Expected config to be numbered 1", 1L, ZKClient
        .extractSuffix("cfg-", children.get(1)));
    assertEquals("Expected config to be numbered 2", 2L, ZKClient
        .extractSuffix("cfg-", children.get(2)));
    store.shutdown();
    client.close();
    zk.shutdown();
    FileUtil.rmr(tmp);
  }

  /**
   * This test creates a zkcs and then hijacks its session through another
   * client. Then we try to use the zkcs to make sure that it's reconnected
   * correctly.
   */
  @Test
  @Ignore("Timing issue prevents this succeeding on Hudson")
  public void testLostSessionOK() throws IOException, InterruptedException,
      KeeperException {
    File tmp = FileUtil.mktempdir();
    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    cfg.setInt(FlumeConfiguration.MASTER_SERVER_ID, 0);

    ZooKeeperService zk = new ZooKeeperService();
    zk.init(cfg);

    ZooKeeperConfigStore store = new ZooKeeperConfigStore(zk);
    store.init();
    String defaultFlowName = cfg.getDefaultFlowName();
    store.setConfig("foo", defaultFlowName, "bar", "baz");
    long sessionid = store.client.zk.getSessionId();
    byte[] sessionpass = store.client.zk.getSessionPasswd();

    // Force session expiration
    Watcher watcher = new Watcher() {

      @Override
      public void process(WatchedEvent event) {
      }

    };
    ZooKeeper zkClient = new ZooKeeper("localhost:2181", 1000, watcher,
        sessionid, sessionpass);
    zkClient.close();

    ZKClient updatingClient = new ZKClient("localhost:2181");
    updatingClient.init();
    Stat stat = new Stat();

    store.client.getChildren("/flume-cfgs", false);

    byte[] bytes = updatingClient.getData("/flume-cfgs/cfg-0000000000", false,
        stat);

    String badCfg = new String(bytes)
        + "\n1,1,default-flow@@bur : null | null;";

    // Force a cfg into ZK to be reloaded by the (hopefully functioning) store
    updatingClient.create("/flume-cfgs/cfg-", badCfg.getBytes(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

    assertTrue(store.client.zk.getSessionId() != sessionid);

    // This sleep is ugly, but we have to wait for the watch to fire
    Clock.sleep(2000);

    assertEquals("null", store.getConfig("bur").getSinkConfig());
  }

  /**
   * Test that Avro-based serialization of phys->logical node maps works
   */
  @Test
  public void testSerializeNodeMap() throws IOException {
    ListMultimap<String, String> nodeMap = ArrayListMultimap
        .<String, String> create();
    nodeMap.put("foo", "bar");
    nodeMap.put("foo", "baz");
    nodeMap.put("foz", "bat");
    byte[] serialized = ZooKeeperConfigStore.serializeNodeMap(nodeMap);
    List<Pair<String, List<String>>> ret = ZooKeeperConfigStore
        .deserializeNodeMap(serialized);

    ListMultimap<String, String> outMap = ArrayListMultimap
        .<String, String> create();
    for (Pair<String, List<String>> p : ret) {
      outMap.putAll(p.getLeft(), p.getRight());
    }
    assertEquals(nodeMap, outMap);
  }

  /**
   * Test that Avro-based serialization of node configs works
   */
  @Test
  public void testSerializeConfigs() throws IOException {
    Map<String, FlumeConfigData> cfgmap = new HashMap<String, FlumeConfigData>();
    FlumeConfigData fcd = new FlumeConfigData();
    fcd.flowID = "my-flow";
    fcd.sinkConfig = "my-sink";
    fcd.sourceConfig = "my-source";
    fcd.timestamp = 10L;
    fcd.sinkVersion = 10;
    fcd.sourceVersion = 100;
    byte[] serialized = ZooKeeperConfigStore.serializeConfigs(cfgmap);

    Map<String, FlumeConfigData> outmap = ZooKeeperConfigStore
        .deserializeConfigs(serialized);

    assertEquals(cfgmap, outmap);
  }
}
