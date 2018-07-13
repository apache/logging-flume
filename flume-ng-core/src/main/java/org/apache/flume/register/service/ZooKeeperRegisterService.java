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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.Context;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Registering Flume Agent with ZooKeeper for service discovery
 * All nodes registered should be ephemeral and and reload usually
 * recreates zookeeper client only if address and properties are changed.
 */
public class ZooKeeperRegisterService extends AbstractRegisterService {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ZooKeeperRegisterService.class);

  public static final String TIER_KEY = "tier";
  public static final String ZK_HOST_KEY = "zkhost";
  public static final String ZK_PORT_KEY = "zkport";
  public static final String ZK_PATH_PREFIX_KEY = "zkpathprefix";
  public static final String ZK_ENDPOINT_PORT_KEY = "zkendpointport";
  public static final String ZK_AUTH_KEY = "zkauth";
  public static final String ZK_MAX_RETRY = "zkmaxretry";
  public static final String ZK_BASE_SLEEP_MS = "zkbasesleepms";

  private static final int DEFAULT_ZK_MAX_RETRY = 3;
  private static final int DEFAULT_ZK_BASE_SLEEP_TIME_MS = 1000;

  private Context context;
  private CuratorFramework client;
  private String zkString;
  private String zkPathPrefix;
  private String zkEndpointPort;
  private String zkAuth;
  private Set<String> tiersPaths;
  private Set<String> previousTiersPaths;
  private boolean overwritePreviousEphemeralNode = true;
  private boolean configured = false;
  
  @Override
  public void start() throws Exception {
    client.start();
    registerZNodes();
  }
  
  private void registerZNodes() throws Exception {
    String tiersString = context.getString(TIER_KEY);
    previousTiersPaths = (previousTiersPaths == null) ?
        new HashSet<String>() : tiersPaths;

    tiersPaths = new HashSet<String>();
    for (String tier : tiersString.split("\\s+")) {
      String zNodePath = zkPathPrefix + "/" + tier;
      tiersPaths.add(zNodePath);
    }

    // Before creating zkPath make sure one does not
    // exist already
    List<String> newTiersPaths = new ArrayList<String>();
    Iterator<String> iter = tiersPaths.iterator();
    while (iter.hasNext()) {
      String tierZNodePath = iter.next();
      if (!previousTiersPaths.contains(tierZNodePath)) {
        newTiersPaths.add(tierZNodePath);
      }
    }

    Set<String> toDeleteTiersPaths = new HashSet<String>();
    for (String path : previousTiersPaths) {
      if (!tiersPaths.contains(path)) {
        toDeleteTiersPaths.add(path);
      }
    }

    String hostname = InetAddress.getLocalHost().getHostName();
    String ephemeralZNodeName = hostname + ":" + zkEndpointPort;

    List<ACL> aclList = null;
    if (zkAuth != null) {
      byte[] d = DigestUtils.sha1(zkAuth);
      String[] userPass = zkAuth.split(":");
      ACL acl = new ACL(ZooDefs.Perms.ALL, new Id("digest", userPass[0] + ":" +
            Base64.encodeBase64(d).toString()));
      aclList = Lists.newArrayList(acl);
      aclList.add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
      aclList.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    } else {
      aclList = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    // Create ephemeral ZNodes for these paths.
    for (String zNode : newTiersPaths) {
      LOGGER.info("Creating peristent parent zNode: " + zNode);
      try {
        client.create().withMode(CreateMode.PERSISTENT).withACL(aclList).forPath(zNode);
      } catch (KeeperException exists) {
        Log.info("zNode exists: " + zNode);
      }
    }

    for (String zNode : tiersPaths) {
      StringBuffer str = new StringBuffer();
      str.append("{\"serviceEndpoint\":{\"host\":\"");
      str.append(hostname);
      str.append("\",\"port\":");
      str.append(zkEndpointPort);
      str.append("},\"additionalEndpoints\":{},\"status\":\"ALIVE\"}");

      String nodePath = zNode + "/" + ephemeralZNodeName;
      LOGGER.info("Creating ephemeral node under zNode: " + zNode +
          " with name: " + nodePath + ", overwrite:" + overwritePreviousEphemeralNode);
      createNode(
          nodePath, str.toString().getBytes(),
          CreateMode.EPHEMERAL, aclList, overwritePreviousEphemeralNode);
    }  

    // Delete ephemeral ZNodes for paths which have been removed
    for (String zNode : toDeleteTiersPaths) {
      String nodePath = zNode + "/" + ephemeralZNodeName;
      LOGGER.info("Deleting ephemeral node under zNode: " + zNode +
          " with name: " + nodePath);
      tryToDeleteNode(nodePath);
    }
  }

  private void tryToDeleteNode(String nodePath) {
    try {
      client.delete().guaranteed().forPath(nodePath);
      LOGGER.info("Delete node:" + nodePath);
    } catch (KeeperException.NoNodeException ex) {
      LOGGER.info("Node doesn't exist:" + nodePath);
    } catch (Exception ex) {
      LOGGER.warn("Delete node throws exception, treated as succeeded", ex);
    }
  }

  private void createNode(
      String path, byte[] data, CreateMode mode, List<ACL> aclList, boolean overwrite)
      throws Exception {
    if (overwrite) {
      LOGGER.info("Delete node if exits" + path);
      tryToDeleteNode(path);
    }
    client.create().creatingParentsIfNeeded()
        .withMode(mode)
        .withACL(aclList)
        .forPath(path, data);
  }

  @Override
  public void stop() {
    configured = false;
    client.close();
  }

  @Override
  public synchronized void configure(Context context) {  
    this.context = context;   
    if (!configured) {
      zkString = context.getString(ZK_HOST_KEY) +
          ":" + context.getString(ZK_PORT_KEY);
      zkPathPrefix = context.getString(ZK_PATH_PREFIX_KEY);
      zkEndpointPort = context.getString(ZK_ENDPOINT_PORT_KEY);
      zkAuth = context.getString(ZK_AUTH_KEY);

      CuratorFrameworkFactory.Builder curatorFactory = CuratorFrameworkFactory.builder();
      if (zkAuth != null) {
        curatorFactory.authorization("digest", zkAuth.getBytes());
      }
      curatorFactory.connectString(zkString);
      int baseSleepTimeMs = context.getInteger(ZK_BASE_SLEEP_MS, DEFAULT_ZK_BASE_SLEEP_TIME_MS);
      int maxRetry = context.getInteger(ZK_MAX_RETRY, DEFAULT_ZK_MAX_RETRY);
      curatorFactory.retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, maxRetry));
      client = curatorFactory.build();
      configured = true;
    }
  }
}
