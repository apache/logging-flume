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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Simple config store that doesn't persist or do anything fancy.
 * 
 * This is is not thread safe.
 */
public class MemoryBackedConfigStore extends ConfigStore {
  final Map<String, FlumeConfigData> cfgs = new HashMap<String, FlumeConfigData>();

  @Override
  public FlumeConfigData getConfig(String host) {
    if (cfgs.containsKey(host)) {
      return cfgs.get(host);
    }

    return null;
  }

  @Override
  public void setConfig(String host, String flowid, String source, String sink)
      throws IOException {
    Preconditions.checkArgument(host != null,
        "Attempted to set config but missing host name!");
    Preconditions.checkArgument(flowid != null, "Attempted to set config "
        + host + " but missing flowid!");
    Preconditions.checkArgument(source != null, "Attempted to set config "
        + host + " but missing source!");
    Preconditions.checkArgument(sink != null, "Attempted to set config " + host
        + " but missing sink");

    long time = Clock.unixTime();
    cfgs.put(host, new FlumeConfigData(time, source, sink, time, time, flowid));
  }

  @Override
  public Map<String, FlumeConfigData> getConfigs() {
    return Collections.unmodifiableMap(cfgs);
  }

  // physnode to logicalNode
  final ListMultimap<String, String> nodeMap = ArrayListMultimap
      .<String, String> create();

  public void addLogicalNode(String physNode, String logicNode) {
    if (nodeMap.containsEntry(physNode, logicNode)) {
      // already present.
      return;
    }
    nodeMap.put(physNode, logicNode);
  }

  public List<String> getLogicalNodes(String physNode) {
    List<String> values;

    values = nodeMap.get(physNode);

    if (values == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(values);
  }

  @Override
  public Multimap<String, String> getLogicalNodeMap() {
    return Multimaps.unmodifiableListMultimap(nodeMap);
  }

  @Override
  public void bulkSetConfig(Map<String, FlumeConfigData> configs)
      throws IOException {
    for (Entry<String, FlumeConfigData> e : configs.entrySet()) {
      FlumeConfigData f = e.getValue();
      setConfig(e.getKey(), f.getFlowID(), f.getSourceConfig(), f
          .getSinkConfig());
    }
  }

  /**
   * Removes the mapping of physNode to a particular logicalNode
   */
  @Override
  public void removeLogicalNode(String logicNode) {
    cfgs.remove(logicNode);
  }

  /**
   * Remove a logical node from the logical node data flow mapping.
   */
  @Override
  public void unmapLogicalNode(String physNode, String logicNode) {
    nodeMap.remove(physNode, logicNode);
  }

  @Override
  public void init() throws IOException, InterruptedException {
    // Nothing to do here
  }

  @Override
  public void shutdown() throws IOException {
    // Nothing to do here
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unmapAllLogicalNodes() {
    // this method should be called relatively rarely.
    ListMultimap<String, String> clone = ArrayListMultimap.create(nodeMap);
    for (Entry<String, String> e : clone.entries()) {
      // reject removing a logical node named the same thing as
      // the physical node.
      if (e.getKey().equals(e.getValue())) {
        continue;
      }
      unmapLogicalNode(e.getKey(), e.getValue());
    }
  }
}
