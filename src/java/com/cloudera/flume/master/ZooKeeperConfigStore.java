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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigDataMap;
import com.cloudera.flume.conf.avro.AvroFlumeNodeMap;
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
  @Override
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
      /*
       * Synchronization notes: from this method, the callback comes from the
       * same thread. That's not guaranteed afterwards, because this gets called
       * again on SessionExpiredException.
       *
       * It tries to take the ZKCS.this lock (in loadConfigs). Therefore BE
       * AWARE of potential deadlocks between threads. The vast majority of the
       * invocations to ZKClient in this class will be under the ZKCS.this lock
       * and therefore thread-safe.
       */
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

  /**
   * Convert a configuration map into an Avro-serialized byte array
   */
  static protected byte[] serializeConfigs(Map<String, FlumeConfigData> cfgs)
      throws IOException {
    Map<CharSequence, AvroFlumeConfigData> map = new HashMap<CharSequence, AvroFlumeConfigData>();
    for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
      AvroFlumeConfigData avroConfig = MasterClientServerAvro.configToAvro(e
          .getValue());

      map.put(new Utf8(e.getKey()), avroConfig);
    }

    AvroFlumeConfigDataMap avromap = new AvroFlumeConfigDataMap();
    avromap.configs = map;

    DatumWriter<AvroFlumeConfigDataMap> datumWriter = new SpecificDatumWriter<AvroFlumeConfigDataMap>();
    datumWriter.setSchema(avromap.getSchema());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataFileWriter<AvroFlumeConfigDataMap> fileWriter = new DataFileWriter<AvroFlumeConfigDataMap>(
        datumWriter);
    fileWriter.create(avromap.getSchema(), baos);
    fileWriter.append(avromap);
    fileWriter.close();
    return baos.toByteArray();
  }

  /**
   * Convert an Avro-serialized byte array into a configuration map
   */
  static protected Map<String, FlumeConfigData> deserializeConfigs(byte[] cfg)
      throws IOException {
    DatumReader<AvroFlumeConfigDataMap> reader = new SpecificDatumReader<AvroFlumeConfigDataMap>();
    reader.setSchema(AvroFlumeConfigDataMap.SCHEMA$);
    DataFileStream<AvroFlumeConfigDataMap> fileStream = new DataFileStream<AvroFlumeConfigDataMap>(
        new ByteArrayInputStream(cfg), reader);
    AvroFlumeConfigDataMap cfgmap = fileStream.next();
    fileStream.close();
    Map<String, FlumeConfigData> ret = new HashMap<String, FlumeConfigData>();
    for (Entry<CharSequence, AvroFlumeConfigData> e : cfgmap.configs.entrySet()) {
      ret.put(e.getKey().toString(), MasterClientServerAvro.configFromAvro(e
          .getValue()));
    }

    return ret;
  }

  /**
   * Writes an Avro-serialized form of all known node configs to ZK
   */
  protected synchronized void saveConfigs(String prefix) {
    Preconditions.checkNotNull(this.client, "Client is null in saveConfigs");

    try {
      String cfgPath = String.format(prefix + "/cfg-%010d", currentVersion);
      // Create a new
      String znode = client.create(cfgPath, serializeConfigs(cfgs),
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
      byte[] cfg = client.getData(path, false, stat);
      if (cfg.length == 0) {
        LOG.info("Config was empty!");
        return;
      }

      cfgs.putAll(deserializeConfigs(cfg));
    } catch (Exception e) {
      throw new IOException("Unexpected exception in loadConfigs", e);
    }
  }

  /**
   * Updates the in-memory cache, and then writes all configs out to ZK
   */
  @Override
  public synchronized void setConfig(String host, String flowid, String source,
      String sink) throws IOException {
    Preconditions.checkArgument(client != null && client.getZK() != null,
        "Attempted to set config but ZK client is not connected!");
    Preconditions.checkArgument(host != null,
        "Attempted to set config but missing hostname!");
    Preconditions.checkArgument(flowid != null, "Attempted to set config "
        + host + " but missing flowid!");
    Preconditions.checkArgument(source != null, "Attempted to set config "
        + host + " but missing source!");
    Preconditions.checkArgument(sink != null, "Attempted to set config " + host
        + " but missing sink!");

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
  @Override
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
  @Override
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

  /**
   * Converts a nodemap into an Avro-serialized byte array
   */
  static protected byte[] serializeNodeMap(ListMultimap<String, String> nodeMap)
      throws IOException {
    DatumWriter<AvroFlumeNodeMap> datumWriter = new SpecificDatumWriter<AvroFlumeNodeMap>();
    AvroFlumeNodeMap avromap = new AvroFlumeNodeMap();

    Map<CharSequence, List<CharSequence>> map = new HashMap<CharSequence, List<CharSequence>>();
    for (Entry<String, Collection<String>> e : nodeMap.asMap().entrySet()) {
      String name = e.getKey();
      GenericArray<CharSequence> out = new GenericData.Array<CharSequence>(e.getValue().size(),
          Schema.createArray(Schema.create(Type.STRING)));

      for (String s : e.getValue()) {
        out.add(new String(s));
      }

      map.put(new String(name), out);
    }

    avromap.nodemap = map;

    datumWriter.setSchema(avromap.getSchema());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataFileWriter<AvroFlumeNodeMap> fileWriter = new DataFileWriter<AvroFlumeNodeMap>(
        datumWriter);
    fileWriter.create(avromap.getSchema(), baos);
    fileWriter.append(avromap);
    fileWriter.close();

    return baos.toByteArray();
  }

  /**
   * Converts an Avro-serialized byte array into a nodemap
   */
  static protected List<Pair<String, List<String>>> deserializeNodeMap(
      byte[] data) throws IOException {
    DatumReader<AvroFlumeNodeMap> reader = new SpecificDatumReader<AvroFlumeNodeMap>();
    DataFileStream<AvroFlumeNodeMap> fileStream = new DataFileStream<AvroFlumeNodeMap>(
        new ByteArrayInputStream(data), reader);
    AvroFlumeNodeMap cfgmap = fileStream.next();
    fileStream.close();

    List<Pair<String, List<String>>> ret = new ArrayList<Pair<String, List<String>>>();

    for (Entry<CharSequence, List<CharSequence>> e : cfgmap.nodemap.entrySet()) {
      List<String> list = new ArrayList<String>();
      for (CharSequence c : e.getValue()) {
        list.add(c.toString());
      }
      ret.add(new Pair<String, List<String>>(e.getKey().toString(), list));
    }
    return ret;
  }

  /**
   * Saves the physical->logical node mappings to ZK. Synchronized so that
   * nodeMap does not have to be copied - it is iterated over by
   * serializeNodeMap
   */
  protected synchronized void saveNodeMaps(String prefix) {
    Preconditions.checkNotNull(this.client, "Client is null in saveNodeMaps");
    try {
      client.create(prefix + "/nodes", serializeNodeMap(this.nodeMap),
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
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
    if (nodeMap.containsEntry(physNode, logicNode)) {
      // already present.
      return;
    }
    nodeMap.put(physNode, logicNode);
    saveNodeMaps(NODEMAPS_PATH);
  }

  @Override
  public synchronized List<String> getLogicalNodes(String physNode) {
    List<String> values;

    values = nodeMap.get(physNode);

    if (values == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(values);
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
  synchronized public void removeLogicalNode(String logicNode)
      throws IOException {
    Preconditions.checkArgument(client != null);
    try {
      currentVersion = zkCounter.incrementAndGet();
    } catch (Exception e) {
      throw new IOException("Could not increment version counter...", e);
    }
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void addChokeLimit(String physNode, String chokeID, int limit) {
    // TODO(Vibhor): Have to add this Zookeeper stuff
    LOG.error("addChoke at ZooKeeper not supported yet");
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Integer> getChokeMap(String physNode) {
    // TODO(Vibhor): Have to add this Zookeeper stuff
    LOG.error("addChoke at ZooKeeper not supported yet");
    throw new UnsupportedOperationException();
  }
}
