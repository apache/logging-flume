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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.FileUtil;


public class TestZooKeeperService {
  
  /**
   * Test that createClient throws when service is not started
   */
  @Test(expected=IOException.class)
  public void testCreateClientFail() throws IOException {
    ZooKeeperService svc = new ZooKeeperService();
    svc.createClient();
  }
  
  /**
   * Test that full init, create client, shutdown lifecycle works correctly
   */
  @Test
  public void testLifecycle() throws IOException, InterruptedException, KeeperException {
    FlumeConfiguration cfg = FlumeConfiguration.get();
    File tmp = FileUtil.mktempdir();
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    ZooKeeperService svc = new ZooKeeperService();
    svc.init(cfg);
    
    Assert.assertTrue("ZooKeeperService did not initialise", svc.isInitialised());
    
    ZKClient client = svc.createClient();
    client.init();
    Assert.assertTrue("Expected at least one child of root for ZK service",
        client.getChildren("/", false).size() > 0);   
    
    client.close();    
    svc.shutdown();    
    Assert.assertFalse("ZooKeeperService did not shut down", svc.isInitialised());
    
    FileUtil.rmr(tmp);
  }
  
  /**
   * Test that we have correctly disabled the maxClientCnxns bound
   */
  @Test
  public void testMultipleClients() throws IOException, InterruptedException, KeeperException {
    FlumeConfiguration cfg = FlumeConfiguration.get();
    File tmp = FileUtil.mktempdir();
    cfg.set(FlumeConfiguration.MASTER_ZK_SERVERS, "localhost:2181:3181:4181");
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmp.getAbsolutePath());
    ZooKeeperService svc = new ZooKeeperService();
    svc.init(cfg);
    
    Assert.assertTrue("ZooKeeperService did not initialise", svc.isInitialised());
    
    List<ZKClient> clients = new ArrayList<ZKClient>();
    for (int i=0;i<15;++i) {
      ZKClient client = svc.createClient();
      client.init();
      clients.add(client);
    }
    
    for (ZKClient client : clients) {
      client.close();
    }
    svc.shutdown();    
    Assert.assertFalse("ZooKeeperService did not shut down", svc.isInitialised());
    
    FileUtil.rmr(tmp);
  }
}
