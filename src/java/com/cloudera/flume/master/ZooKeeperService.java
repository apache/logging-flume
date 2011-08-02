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

import java.io.IOException;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This class provides a singleton interface to a ZooKeeper ensemble, supporting
 * either in-process or external ZK servers.
 */
public class ZooKeeperService {
  static final Logger LOG = LoggerFactory.getLogger(ZooKeeperService.class);
  boolean external;
  ZKInProcessServer zk = null;
  boolean initialised = false;
    
  // A string of hostname:clientport:electionport tuples
  String serverAddr = null;
  
  // The string to use to connect clients to this service
  String clientAddr = null;

  // The singleton ZooKeeperService
  final static ZooKeeperService zkServiceSingleton = new ZooKeeperService();

  /**
   * Reads startup parameters from a configuration file and does one of the
   * following:
   * 
   * 1. start a standalone, in-process server
   * 
   * 2. start an in-process server that's part of an ensemble
   * 
   * 3. does nothing, as the service is a proxy for an entirely external cluster
   * 
   * If called twice in succession, the second call will do nothing.
   */
  synchronized protected void init(FlumeConfiguration cfg) throws IOException,
      InterruptedException {
    if (initialised) {
      return;
    }
    serverAddr = cfg.getMasterZKServers();
    clientAddr = cfg.getMasterZKConnectString();
    external = true;
    if (!cfg.getMasterZKUseExternal()) {
      external = false;
      if (cfg.getMasterIsDistributed()) {
        try {
          startZKDistributed(cfg);
        } catch (ConfigException e) {
          throw new IOException("Couldn't parse ZooKeeper configuration", e);
        }
      } else {
        int port = cfg.getMasterZKClientPort();
        String dir = cfg.getMasterZKLogDir();
        startZKStandalone(port, dir);
      }
    }
    initialised = true;
  }

  synchronized public boolean isInitialised() {
    return initialised;
  }

  /**
   * Initialises in standalone mode, creating an in-process ZK.
   */
  protected void startZKStandalone(int port, String dir) throws IOException,
      InterruptedException {
    LOG.info("Starting standalone ZooKeeper instance on port " + port);
    zk = new ZKInProcessServer(port, dir);
    zk.start();
  }

  /**
   * Initialises in distributed mode, starting an in-process server
   */
  protected void startZKDistributed(FlumeConfiguration cfg) throws IOException,
      InterruptedException, ConfigException {
    LOG.info("Starting ZooKeeper server as part of ensemble");
    zk = new ZKInProcessServer(cfg);
    zk.start();
  }

  /**
   * Returns the singleton ZooKeeperService, which will not be null. However, it
   * may not be initialised and therefore attempts to connect to it with a
   * ZKClient may fail. Use getAndInit if you are not sure whether the service
   * is initialised.
   */
  static public ZooKeeperService get() {
    return zkServiceSingleton;
  }

  /**
   * Returns the singleton ZooKeeperService, initialising it if it has not been
   * initialised already.
   */
  synchronized public static ZooKeeperService getAndInit() throws IOException,
      InterruptedException {
    zkServiceSingleton.init(FlumeConfiguration.get());
    return zkServiceSingleton;
  }

  /**
   * Returns the singleton ZooKeeperService, initialising it if it has not been
   * initialised already, using the supplied configuration
   */
  synchronized public static ZooKeeperService getAndInit(FlumeConfiguration cfg)
      throws IOException, InterruptedException {
    zkServiceSingleton.init(cfg);
    return zkServiceSingleton;
  }

  /**
   * Returns a new ZKClient initialised for this service, but not connected.
   */
  synchronized public ZKClient createClient() throws IOException {
    if (!initialised) {
      throw new IOException("Not yet initialised!");
    }
    return new ZKClient(clientAddr);
  }

  /**
   * Shuts down any in-process ZooKeeper instance
   */
  synchronized public void shutdown() {
    if (!initialised) {
      return;
    }
    if (!external && this.zk != null) {
      this.zk.stop();
    }
    this.zk = null;
    this.initialised = false;
  }
}
