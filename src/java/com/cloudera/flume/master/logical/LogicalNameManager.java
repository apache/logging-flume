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

package com.cloudera.flume.master.logical;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumePatterns;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This provides physical node information when logical names are provided or
 * supplied.
 */
public class LogicalNameManager {

  final ConfigurationManager cfgMan;
  final StatusManager statMan;

  public LogicalNameManager(ConfigurationManager cfg, StatusManager stat) {
    this.cfgMan = cfg;
    this.statMan = stat;
  }

  /**
   * This class has the ability to generate physical sources and physical sinks
   */
  abstract public static class PhysicalNodeInfo {
    public abstract String getPhysicalSource();

    public abstract String getPhysicalSink();
  };

  /**
   * This uses the default rpc source / sink. Right now we assume only network
   * connections are possible.
   * 
   * TODO optimization for in-process fifo/queue for in memory object passing.
   */
  public static class RpcPhysicalNode extends PhysicalNodeInfo {
    String host;
    int port;

    RpcPhysicalNode(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public String getPhysicalSink() {
      return "rpcSink(\"" + host + "\"," + port + ")";
    }

    @Override
    public String getPhysicalSource() {
      return "rpcSource(" + port + ")";
    }

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

  Map<String, PhysicalNodeInfo> nameMap = new HashMap<String, PhysicalNodeInfo>();
  Multimap<String, Integer> portMaps = HashMultimap.<String, Integer> create();

  synchronized void updateNode(String ln) throws RecognitionException {
    FlumeConfigData fcd = cfgMan.getConfig(ln);
    if (fcd == null) {
      return;
    }

    NodeStatus stat = statMan.getStatus(ln);
    if (stat == null) {
      // only get live nodes (or formerly live nodes)

      // set entry to null.
      nameMap.put(ln, null);
      // TODO (jon) need to unassign port from node.
      return;
    }

    PhysicalNodeInfo pn = nameMap.get(ln);
    if (pn != null) {
      return; // already assigned, skip
    }

    // right now only support host:port
    String host = stat.host;
    String src = fcd.getSourceConfig();
    CommonTree lsrc = FlumePatterns.findSource(src, "logicalSource");
    if (lsrc == null) {
      // no logical sources here, skip
      return;
    }

    // make sure no two logical nodes on the same host have the same port
    int port = FlumeConfiguration.get().getCollectorPort();
    Collection<Integer> ports = portMaps.get(host);
    while (ports.contains(port)) {
      port++;
    }

    // / build and populate name Map.
    pn = new RpcPhysicalNode(host, port);
    portMaps.put(host, port);
    nameMap.put(ln, pn);
  }

  /**
   * This updates all the logical sources by associating them with a hostname
   * and assigning ports to each logical node.
   */
  synchronized void update() throws RecognitionException {
    Map<String, FlumeConfigData> cfgs = cfgMan.getAllConfigs();

    // sorting to make this deterministic for testing
    cfgs = new TreeMap<String, FlumeConfigData>(cfgs);

    // process sources.
    for (String ln : cfgs.keySet()) {
      updateNode(ln);
    }

  }

  /**
   * Get the PhysicalNode info associated with the specified logical node
   */
  synchronized public PhysicalNodeInfo getPhysicalNodeInfo(String logicalNode) {
    return nameMap.get(logicalNode);
  }

  /**
   * Set the PhysicalNode info associated with the specified logical node
   */
  synchronized public void setPhysicalNode(String logicalNode,
      PhysicalNodeInfo pn) {
    nameMap.put(logicalNode, pn);
  }

}
