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

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.collect.Multimap;

/**
 * This is an interface for the object that manages configurations for nodes in
 * flume.
 * 
 * TODO (jon) we should be consistent with our naming. Maybe Configuration for
 * data flow configurations, and properties for flume-site.xml things.
 * 
 * TODO (jon) this has a lot of overlap with ConfigStore (I prefer interfaces
 * personally). Maybe they should be unified.
 */
public interface ConfigurationManager extends Reportable {
  public FlumeConfigData getConfig(String host);

  public void start() throws IOException;

  public void stop() throws IOException;

  /**
   * change or add a data flow configuration for node host.
   */
  public void setConfig(String host, String flowid, String source, String sink)
      throws IOException, FlumeSpecException;

  /**
   * Load configurations from file 'from'. This does not clear prexisting
   * configurations but may overwrite configurations for existing nodes.
   */
  public void loadConfigFile(String from) throws IOException;

  /**
   * Save a configuration to file 'from'.
   */
  public void saveConfigFile(String from) throws IOException;

  /**
   * Update many configurations in one operation
   */
  public void setBulkConfig(Map<String, FlumeConfigData> configs)
      throws IOException;

  /**
   * Gets an unmodifiable map from logical node to dataflow configurations.
   */
  public Map<String, FlumeConfigData> getAllConfigs();

  /**
   * Get all the translated logical node configurations.
   */
  public Map<String, FlumeConfigData> getTranslatedConfigs();

  /**
   * Gets an unmodifiable list of all of the logical nodes associated with the
   * specified physical node
   */
  public List<String> getLogicalNode(String physNode);

  /**
   * Associates a new logical node to the specified physical node. If no
   * physical node exists, it is created as well.
   */
  public void addLogicalNode(String physNode, String logicNode);

  /**
   * This removes the logical node data flow configuration from both the flow
   * table and the phys-logical mapping
   */
  public void removeLogicalNode(String logicNode) throws IOException;

  /**
   * This removes the mapping from a physical node to the logical node, but
   * leaves the logicalNode data flow configuration.
   */
  public void unmapLogicalNode(String physNode, String logicNode);

  /**
   * Gets the physical node associated with a logical node.
   */
  public String getPhysicalNode(String logicalNode);

  /**
   * Gets the full physical to logical nodes mapping
   */
  public Multimap<String, String> getLogicalNodeMap();

  /**
   * Refreshes the configuration of a logical node. (updates version to force
   * node to reload)
   */
  public void refresh(String logicalNode) throws IOException;

  /**
   * Refreshes all of the configuration of all of the logical nodes. (updates
   * version to force node to reload)
   */
  public void refreshAll() throws IOException;

  /**
   * Similar to refreshAll, except it only incrementally refreshes
   * configurations that change due to state changes. This is only really
   * necessary for translating configuration managers that rely on state when
   * translating.
   * 
   * Example: In a storage configuration manager, nothing happens.
   * 
   * Example: In a failover manager, if a collector is reconfigured to be
   * something else, it is removed from the fail chain manager which changes the
   * output of a translator. Only those configurations that relied on the the
   * removed collector get updated.
   */
  public void updateAll() throws IOException;

  /**
   * Unmaps all logical nodes in a single operation.
   */
  public void unmapAllLogicalNodes() throws IOException;

}
