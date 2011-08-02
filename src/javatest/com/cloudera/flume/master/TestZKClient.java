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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * A variety of tests both for ZKClient and the ZKServer
 * 
 * These tests spew exceptions ALL OVER THE PLACE. These are part of normal
 * zookeeper operation - the key is whether the tests pass or fail.
 */
public class TestZKClient {
  final static Logger LOG = Logger.getLogger(TestZKClient.class);

  /**
   * Tests bringing up and shutting down a standalone ZooKeeper instance
   */
  @Test
  public void testStandaloneUpAndDown() throws Exception {
    ZKInProcessServer zk = new ZKInProcessServer(2181, "/tmp/flume-test-zk/");
    zk.start();
    ZooKeeper client = new ZooKeeper("localhost:2181", 5000, new Watcher() {
      public void process(WatchedEvent event) {
      }
    });

    Thread.sleep(1000);
    List<String> children = client.getChildren("/", false);
    LOG.warn("Got " + children.size() + " children");
    assertTrue(children.size() > 0);
    zk.stop();
  }

  /**
   * Test bringing up several in-process servers to form an ensemble.
   */
  @Test
  public void testMultipleDistributedUpAndDown() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("tickTime", "2000");
    properties.setProperty("initLimit", "10");
    properties.setProperty("syncLimit", "5");
    properties.setProperty("maxClientCnxns", "0");
    ZKInProcessServer[] zks = new ZKInProcessServer[3];
    properties.setProperty("server.0", "localhost:3181");
    properties.setProperty("server.1", "localhost:3182");
    properties.setProperty("server.2", "localhost:3183");
    properties.setProperty("electionAlg", new Integer(0).toString());

    for (int i = 0; i < 3; ++i) {
      LOG.info("Starting server " + i);
      properties.setProperty("dataDir", "/tmp/flume-test-zk/datadir" + i);
      properties.setProperty("dataLogDir", "/tmp/flume-test-zk/datadir" + i
          + "/logs");
      properties.setProperty("clientPort", new Integer(2181 + i).toString());
      properties.setProperty("serverID", "" + i);
      zks[i] = new ZKInProcessServer(properties);
      if (i < 2) {
        zks[i].startWithoutWaiting();
      } else {
        zks[i].start();
      }
    }

    ZKClient client = new ZKClient("localhost:2181");
    client.init();
    assertTrue(client.getChildren("/", false).size() > 0);
    for (ZKInProcessServer z : zks) {
      z.stop();
    }
  }

  /**
   * Test a number of ZKClient operations
   */
  @Test
  public void testZKClient() throws Exception {
    File temp = File.createTempFile("flume-zk-test", "");
    temp.delete();
    temp.mkdir();
    temp.deleteOnExit();
    ZKInProcessServer zk = new ZKInProcessServer(3181, temp.getAbsolutePath());
    zk.start();
    ZKClient client = new ZKClient("localhost:3181");
    client.init();
    client.ensureDeleted("/test-flume", -1);
    assertTrue("Delete not successful!",
        client.exists("/test-flume", false) == null);
    client.ensureExists("/test-flume", "hello world".getBytes());
    Stat stat = new Stat();
    byte[] data = client.getData("/test-flume", false, stat);
    String dataString = new String(data);
    assertEquals("Didn't get expected 'hello world' from getData - "
        + dataString, dataString, "hello world");

    client.delete("/test-flume", stat.getVersion());
    assertTrue("Final delete not successful!", client.exists("/test-flume",
        false) == null);
    zk.stop();
  }

  /**
   * Test failure modes - should throw KeeperException when can't get to ZK.
   */
  @Test(expected = KeeperException.class)
  public void testZKClientFailures() throws IOException, KeeperException,
      InterruptedException {
    ZKClient client = null;
    try {
      File temp = File.createTempFile("flume-zk-test", "");
      temp.delete();
      temp.mkdir();
      temp.deleteOnExit();
      ZKInProcessServer zk = new ZKInProcessServer(3181, temp.getAbsolutePath());
      zk.start();
      client = new ZKClient("localhost:3181");
      client.init();
      client.ensureDeleted("/dummy", -1);
      zk.stop();
    } catch (IOException e) {
      assertTrue("IOException caught! " + e, false);
    }

    client.create("/dummy", new byte[0], Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  /**
   * Test sequential child commands
   */
  @Test
  public void testZKSequentialChildren() throws InterruptedException,
      IOException, KeeperException {
    File temp = File.createTempFile("flume-zk-test", "");
    temp.delete();
    temp.mkdir();
    temp.deleteOnExit();
    ZKInProcessServer zk = new ZKInProcessServer(3181, temp.getAbsolutePath());
    zk.start();
    ZKClient client = new ZKClient("localhost:3181");
    client.init();
    client.ensureExists("/seqtests", new byte[0]);
    for (int i = 0; i < 10; ++i) {
      client.create("/seqtests/seqnode", new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    String child = client.getLastSequentialChild("/seqtests", "seqnode", false);
    assertEquals("Expected to get seqnode0000000009, got " + child,
        "seqnode0000000009", child);
  }
}
