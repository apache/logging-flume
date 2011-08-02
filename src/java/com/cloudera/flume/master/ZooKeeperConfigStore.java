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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.master.ZKClient.InitCallback;
import com.cloudera.util.Clock;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 * ZooKeeper based store for node configuration.
 */
public class ZooKeeperConfigStore extends ConfigStore implements Watcher {
  ZKClient client = null;
  final Map<String, FlumeConfigData> cfgs = new HashMap<String, FlumeConfigData>();
  ListMultimap<String, String> nodeMap = ArrayListMultimap
      .<String, String> create();
  final static Logger LOG = Logger.getLogger(ZooKeeperConfigStore.class);
  final static String CFGS_PATH = "/flume-cfgs";
  final static String NODEMAPS_PATH = "/flume-nodes";

  // Tracks the version number of each config
  ZooKeeperCounter zkCounter;
  long currentVersion = -1;

  ZooKeeperService zkService = null;

  /**
   * Constructs a new ZooKeeperConfigStore, using the system-wide
   * ZooKeeperService
   */
  public ZooKeeperConfigStore() {
    this(ZooKeeperService.get());
  }

  /**
   * Exists so that we can control the ZooKeeperService, for testing purposes.
   */
  protected ZooKeeperConfigStore(ZooKeeperService zooKeeperService) {
    this.zkService = zooKeeperService;
  }

  /**
   * Reads the standard configuration and initialises client and optionally
   * server accordingly.
   */
  public void init() throws IOException, InterruptedException {
    Preconditions.checkArgument(this.zkService != null,
        "ZooKeeperService is null in init");
    connect();
  }

  /**
   * Connect to an ensemble and load the configs / nodemaps
   */
  protected synchronized void connect() throws IOException,
      InterruptedException {
    Preconditions.checkState(zkService.isInitialised(),
        "ZooKeeperService not initialised in ZKCS.connect()");
    if (client != null) {
      client.getZK().close();
    }
    InitCallback cb = new InitCallback() {
      @Override
      public void success(ZKClient client) throws IOException {
        client.getZK().register(ZooKeeperConfigStore.this);

        loadConfigs(CFGS_PATH);
        loadNodeMaps(NODEMAPS_PATH);
      }
    };

    client = zkService.createClient();
    client.init(cb);
    try {
      ZooKeeperConfigStore.this.zkCounter = new ZooKeeperCounter(
          this.zkService, "/counters-config_version");
    } catch (KeeperException e) {
      throw new IOException("Couldn't create ZooKeeperCounter!", e);
    } catch (InterruptedException e) {
      throw new IOException("Couldn't create ZooKeeperCounter!", e);
    }
  }

  protected synchronized void saveConfigs(String prefix) {
    Preconditions.checkNotNull(this.client, "Client is null in saveConfigs");
    StringBuilder sb = new StringBuilder();
    try {
      for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
        String name = e.getKey();
        FlumeConfigData f = e.getValue();
        String snk = f.getSinkConfig();
        String src = f.getSourceConfig();
        sb.append(f.getSourceVersion() + "," + f.getSinkVersion() + ","
            + f.getFlowID() + "@@");
        sb.append(FlumeBuilder.toLine(name, src, snk));
        sb.append("\n");
      }

      String cfgPath = String.format(prefix + "/cfg-%010d", currentVersion);

      // Create a new
      String znode = client.create(cfgPath, sb.toString().getBytes(),
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      LOG.info("Created new config at " + znode);
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception: ", e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while saving config", e);
    } catch (IOException e) {
      LOG.error("IOException when saving config", e);
    }
  }

  /**
   * This internal method is called at connection time to populate the cache.
   * 
   * May be called from either the main Master thread or a ZK-initiated callback
   * so is synchronized to prevent racing.
   */
  synchronized protected void loadConfigs(String prefix) throws IOException {
    // Finds the most recent prefix
    try {
      client.ensureExists(prefix, new byte[0]);

      /*
       * We can't use the counter value here because there may have been failed
       * attempts to write a config - so the config sequence will not be
       * continuous.
       */
      String latest = client.getLastSequentialChild(prefix, "cfg-", true);
      String path = prefix + "/" + latest;

      if (latest == null) {
        LOG.debug("No configurations found");
        return;
      }

      long latestVersion = ZKClient.extractSuffix("cfg-", path);

      if (currentVersion == latestVersion) {
        LOG.debug("Trying to load current version, ignoring");
        return;
      }

      cfgs.clear();

      Stat stat = new Stat();
      LOG.info("Loading config from: " + path);
      String cfg = new String(client.getData(path, false, stat));

      if (cfg.equals("")) {
        LOG.debug("Empty configuration");
        return;
      }

      for (String data : cfg.split("\n")) {
        // Nodes are serialised in the form
        // srcVersion,snkVersion,flowid@@cfgString
        String[] parts = data.split("@@");
        Preconditions.checkState(parts.length == 2,
            "Malformed node configuration: " + data);
        String[] versions = parts[0].split(",");
        Preconditions.checkState(versions.length == 3,
            "Malformed version numbers: " + parts[0]);
        long srcVersion = Long.parseLong(versions[0]);
        long snkVersion = Long.parseLong(versions[1]);
        String flowid = versions[2];
        List<FlumeNodeSpec> confs = FlumeSpecGen.generate(parts[1]);

        for (FlumeNodeSpec spec : confs) {
          cfgs.put(spec.node, new FlumeConfigData(Clock.unixTime(), spec.src,
              spec.sink, srcVersion, snkVersion, flowid));
        }
      }
    } catch (Exception e) {
      throw new IOException("Unexpected exception in loadConfigs", e);
    }
  }

  /**
   * Updates the in-memory cache, and then writes all configs out to ZK
   */
  public synchronized void setConfig(String host, String flowid, String source,
      String sink) throws IOException {
    Preconditions.checkArgument(client != null && client.getZK() != null,
        "client connection is null in setConfig");

    if (client.getZK().getState() != ZooKeeper.States.CONNECTED) {
      throw new IOException("Not connected to ZooKeeper: "
          + client.getZK().getState());
    }

    try {
      currentVersion = zkCounter.incrementAndGet();
    } catch (Exception e) {
      throw new IOException("Could not increment version counter...", e);
    }

    cfgs.put(host, new FlumeConfigData(Clock.unixTime(), source, sink,
        currentVersion, currentVersion, flowid));
    saveConfigs(CFGS_PATH);
  }

  /**
   * Saves a list of configuration updates with as one new configuration -
   * avoids multiple watches getting fired.
   */
  public synchronized void bulkSetConfig(Map<String, FlumeConfigData> configs)
      throws IOException {
    Preconditions.checkArgument(client != null && client.getZK() != null);

    if (client.getZK().getState() != ZooKeeper.States.CONNECTED) {
      throw new IOException("Not connected to ZooKeeper: "
          + client.getZK().getState());
    }

    try {
      currentVersion = zkCounter.incrementAndGet();
    } catch (Exception e) {
      throw new IOException("Could not increment version counter...", e);
    }

    for (Entry<String, FlumeConfigData> e : configs.entrySet()) {
      FlumeConfigData f = new FlumeConfigData(Clock.unixTime(), e.getValue()
          .getSourceConfig(), e.getValue().getSinkConfig(), currentVersion,
          currentVersion, e.getValue().getFlowID());
      cfgs.put(e.getKey(), f);
    }
    saveConfigs(CFGS_PATH);
  }

  /**
   * Checks the in-memory cache, but does not go to ZooKeeper to check.
   */
  public synchronized FlumeConfigData getConfig(String host) {
    Preconditions.checkArgument(client != null);
    if (cfgs.containsKey(host)) {
      return cfgs.get(host);
    }

    return null;
  }

  @Override
  public synchronized Map<String, FlumeConfigData> getConfigs() {
    return Collections.unmodifiableMap(cfgs);
  }

  // TODO (jon) use more robust mechanism to serialize list (split will drop
  // last "")

  /**
   * Utility commands to (de)serialize nodemaps as strings
   */
  protected String nodeMapAsString(String physNode,
      Collection<String> logicalNodes) {
    StringBuilder sb = new StringBuilder();
    sb.append(physNode);
    sb.append(',');
    sb.append(StringUtils.join(logicalNodes, ','));
    return sb.toString();
  }

  /**
   * Needs synchronized for iterating over nodeMap
   */
  synchronized protected byte[] serializeNodeMap() {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, Collection<String>> e : nodeMap.asMap().entrySet()) {
      String name = e.getKey();
      Collection<String> logicalNodes = e.getValue();
      sb.append(nodeMapAsString(name, logicalNodes));
      sb.append("\n");
    }
    return sb.toString().getBytes();
  }

  /**
   * Deserialization utility
   */
  protected Pair<String, List<String>> nodeMapFromString(String line) {
    String[] parsed = line.split(",");

    List<String> lns = new ArrayList<String>();
    lns.addAll(Arrays.asList(parsed)); // asList alone returns unmutable list.
    lns.remove(0);
    return new Pair<String, List<String>>(parsed[0], lns);
  }

  protected List<Pair<String, List<String>>> deserializeNodeMap(byte[] data) {
    String str = new String(data);
    List<Pair<String, List<String>>> ret = new ArrayList<Pair<String, List<String>>>();
    for (String s : str.split("\n")) {
      ret.add(nodeMapFromString(s));
    }
    return ret;
  }

  /**
   * Saves the physical->logical node mappings to ZK
   */
  protected synchronized void saveNodeMaps(String prefix) {
    Preconditions.checkNotNull(this.client, "Client is null in saveNodeMaps");
    try {
      client.create(prefix + "/nodes", serializeNodeMap(), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception: ", e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while saving config", e);
    } catch (IOException e) {
      LOG.error("IOException when saving config", e);
    }
  }

  /**
   * Loads the physical->logical node mappings from ZK
   */
  protected synchronized void loadNodeMaps(String prefix) throws IOException {
    Preconditions.checkNotNull(this.client, "Client is null in loadNodeMaps");
    // Finds the most recent prefix
    try {
      client.ensureExists(prefix, new byte[0]);
      String latest = client.getLastSequentialChild(prefix, "nodes", true);
      if (latest == null) {
        LOG.info("No nodemaps found at " + prefix + "/nodes*");
        return;
      }
      nodeMap.clear(); // reset prior to reload
      String path = prefix + "/" + latest;
      LOG.info("Loading nodes from: " + path);
      Stat stat = new Stat();
      byte[] data = client.getData(path, false, stat);
      for (Pair<String, List<String>> mapping : deserializeNodeMap(data)) {
        nodeMap.putAll(mapping.getLeft(), mapping.getRight());
      }
    } catch (Exception e) {
      LOG.error(e, e);
      throw new IOException("Unexpected exception in loadNodes", e);
    }
  }

  @Override
  public synchronized void addLogicalNode(String physNode, String logicNode) {
    Preconditions.checkArgument(client != null);
    nodeMap.put(physNode, logicNode);
    saveNodeMaps(NODEMAPS_PATH);
  }

  @Override
  public synchronized List<String> getLogicalNodes(String physNode) {
    return Collections.unmodifiableList(nodeMap.get(physNode));
  }

  @Override
  /**
   * This is called whenever an event is seen on the ZK ensemble that we
   * have registered for. We care particularly about changes to the list of
   * configurations, made by some other peer.  
   */
  public synchronized void process(WatchedEvent event) {
    if (client == null) {
      // This is the 'death-rattle' callback, made when we close the client.
      return;
    }
    LOG.debug("Saw ZooKeeper watch event " + event);
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      if (event.getPath().equals(CFGS_PATH)) {
        try {
          LOG.info("Config was updated - reloading");
          loadConfigs(CFGS_PATH);
        } catch (IOException e) {
          LOG.error("IOException when reloading configs", e);
        }
      }
      if (event.getPath().equals(NODEMAPS_PATH)) {
        try {
          LOG.info("Nodemaps were updated - reloading");
          loadNodeMaps(NODEMAPS_PATH);
        } catch (IOException e) {
          LOG.error("IOException when reloading nodemaps", e);
        }
      }
    }
  }

  @Override
  synchronized public ListMultimap<String, String> getLogicalNodeMap() {
    return Multimaps.unmodifiableListMultimap(nodeMap);
  }

  /**
   * Remove a logical node from the logical node data flow mapping.
   */
  @Override
  synchronized public void removeLogicalNode(String logicNode) {
    Preconditions.checkArgument(client != null);
    cfgs.remove(logicNode);
    saveConfigs(CFGS_PATH);
  }

  /**
   * Removes the mapping of physNode to a particular logicalNode
   */
  @Override
  synchronized public void unmapLogicalNode(String physNode, String logicNode) {
    Preconditions.checkArgument(client != null);
    nodeMap.remove(physNode, logicNode);
    saveNodeMaps(NODEMAPS_PATH);
  }

  @Override
  synchronized public void shutdown() throws IOException {
    // When we shutdown, there is a callback that is made that we wish to
    // ignore. We signal this by setting client = null, before closing the
    // connection. But we want this to be published to the thread that receives
    // the callback, hence the synchronization to guarantee that ordering.
    if (client != null) {
      ZKClient oldClient = client;
      client = null;
      try {
        oldClient.close();
      } catch (InterruptedException e) {
        LOG.warn("Client interrupted when shutting down connection to ZK");
      }
      try {
        zkCounter.shutdown();
      } catch (InterruptedException e) {
        LOG.warn("Counter interrupted when shutting down connection to ZK");
      }
      client = null;
      zkCounter = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unmapAllLogicalNodes() {
    ListMultimap<String, String> clone = null;
    // create is not thread safe
    synchronized (this) {
      clone = ArrayListMultimap.create(nodeMap);
    }
    for (Entry<String, String> e : clone.entries()) {
      // reject removing a logical node named the same thing as
      // the physical node.
      if (e.getKey().equals(e.getValue())) {
        continue;
      }
      unmapLogicalNode(e.getKey(), e.getValue());
    }

    saveNodeMaps(NODEMAPS_PATH);
  }
}
