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
import java.util.List;
import java.util.Map;

import com.cloudera.flume.conf.FlumeConfigData;
import com.google.common.collect.Multimap;

/**
 * This class abstracts away the persistence of global node config data.
 */
abstract public class ConfigStore {

  abstract public void init() throws IOException, InterruptedException;

  abstract public void shutdown() throws IOException;

  abstract public void setConfig(String host, String flowid, String source,
      String sink) throws IOException;

  /**
   * This retrieves a FlumeConfigData for the specified logical node. If not
   * present returns null.
   */
  abstract public FlumeConfigData getConfig(String logical);

  /**
   * Returns a map from logical node to FlumeConfigData for all data flows
   */
  abstract public Map<String, FlumeConfigData> getConfigs();

  /**
   * This adds a logical node to a physical node.
   */
  abstract public void addLogicalNode(String physNode, String logicNode);

  /**
   * This get the list of logical nodes associated with a physical node.
   */
  abstract public List<String> getLogicalNodes(String physNode);

  /**
   * This gets the entire mapping of physical nodes to sets of logical nodes.
   */
  abstract public Multimap<String, String> getLogicalNodeMap();

  /**
   * Set a collection of configurations at once atomically. Configurations not
   * in this map are left unchanged.
   */
  abstract public void bulkSetConfig(Map<String, FlumeConfigData> configs)
      throws IOException;

  /**
   * Removes the mapping of physNode to a particular logicalNode
   */
  abstract public void unmapLogicalNode(String physNode, String logicNode);

  /**
   * Remove a logical node from the logical node data flow mapping. 
   */
  abstract public void removeLogicalNode(String logicNode) throws IOException;

  /**
   * Unmaps all logical nodes from all physical nodes, except for the main
   * logical node. (logicalnnodename == physicalnodename)
   */
  abstract public void unmapAllLogicalNodes();

}
