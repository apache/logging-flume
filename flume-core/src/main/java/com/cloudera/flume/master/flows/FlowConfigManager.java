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
package com.cloudera.flume.master.flows;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.availability.ConsistentHashFailoverChainManager;
import com.cloudera.flume.master.failover.FailoverConfigurationManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;

/**
 * This maintains separate ConfigurationMangers per flow.
 */
abstract public class FlowConfigManager implements ConfigurationManager {

  /**
   * The parent config manager. This is likely a durable manager.
   */
  final ConfigurationManager parent;

  /**
   * Map from flowid to ConfigurationManager
   */
  final Map<String, ConfigurationManager> flows = new HashMap<String, ConfigurationManager>();

  /**
   * Constructs a new FlowConfigManager. The specified parent Configuration
   * manager is a ConfigurationManager that will track *all configurations*
   * (likely durably). The data in the parent is not partitioned.
   */
  public FlowConfigManager(ConfigurationManager parent) {
    Preconditions.checkArgument(parent != null);
    this.parent = parent;
  }

  /**
   * returns the flow id of a particular logicalNode
   */
  synchronized public String getFlowId(String logicalNode) {
    FlumeConfigData fcd = parent.getConfig(logicalNode);
    if (fcd == null) {
      return null;
    }
    return fcd.flowID;
  }

  /**
   * Only for testing
   */
  synchronized public ConfigurationManager getConfigManForFlow(String flowid) {
    return flows.get(flowid);
  }

  /**
   * Returns a config man for a flow (never returns null). This is not thread
   * safe and must be run guarded by the this lock.
   */
  ConfigurationManager getCreateFlowConfigMan(String flowid) {
    ConfigurationManager cfg = flows.get(flowid);
    if (cfg != null) {
      return cfg;
    }

    cfg = createConfigMan();
    flows.put(flowid, cfg);
    return cfg;

  }

  /**
   * This creates a specific instance of a Configuration manager that will only
   * receive FCD's for a particular flow. This shall never return null. THis
   * does not need to be guarded by a lock.
   */
  public abstract ConfigurationManager createConfigMan();

  /**
   * Adds to the parent, and then adds to the specific flow's configuration
   * manager
   */
  @Override
  synchronized public boolean addLogicalNode(String physNode, String logicNode) {
    boolean result;

    result = parent.addLogicalNode(physNode, logicNode);
    String flowid = getFlowId(logicNode);
    ConfigurationManager fcfg = getCreateFlowConfigMan(flowid);
    fcfg.addLogicalNode(physNode, logicNode);

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void addChokeLimit(String physNode, String chokeID,
      int limit) {
    parent.addChokeLimit(physNode, chokeID, limit);

  }

  /**
   * {@inheritDoc}
   * 
   * This actually takes all the specified configs, buckets them by flowid, and
   * then uses bulk config updates on the corresponding flow configmanagers.
   * **/
  @Override
  synchronized public void setBulkConfig(Map<String, FlumeConfigData> configs)
      throws IOException {
    parent.setBulkConfig(configs);
    // create buckets for each flow id.
    // flow -> (ln, fcd)
    Map<String, Map<String, FlumeConfigData>> flowsets = new HashMap<String, Map<String, FlumeConfigData>>();
    for (Entry<String, FlumeConfigData> e : configs.entrySet()) {
      String node = e.getKey();
      FlumeConfigData fcd = e.getValue();
      String flow = fcd.flowID;
      Map<String, FlumeConfigData> cfgs = flowsets.get(flow);
      if (cfgs == null) {
        cfgs = new HashMap<String, FlumeConfigData>();
        flowsets.put(flow, cfgs);
      }
      cfgs.put(node, fcd);
    }

    // do a bulk update for each sub flow config manager
    for (Entry<String, Map<String, FlumeConfigData>> e : flowsets.entrySet()) {
      String flow = e.getKey();
      ConfigurationManager fcfg = getCreateFlowConfigMan(flow);
      fcfg.setBulkConfig(e.getValue());
    }
  }

  @Override
  synchronized public Map<String, FlumeConfigData> getAllConfigs() {
    return parent.getAllConfigs();
  }

  @Override
  synchronized public FlumeConfigData getConfig(String logicNode) {
    String flowid = getFlowId(logicNode);
    ConfigurationManager fcfg = getCreateFlowConfigMan(flowid);
    return fcfg.getConfig(logicNode);
  }

  @Override
  synchronized public List<String> getLogicalNode(String physNode) {
    return parent.getLogicalNode(physNode);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public Map<String, Integer> getChokeMap(String physNode) {
    return parent.getChokeMap(physNode);
  }

  @Override
  synchronized public Multimap<String, String> getLogicalNodeMap() {
    return parent.getLogicalNodeMap();
  }

  @Override
  synchronized public String getPhysicalNode(String logicalNode) {
    return parent.getPhysicalNode(logicalNode);
  }

  @Override
  synchronized public Map<String, FlumeConfigData> getTranslatedConfigs() {
    Map<String, FlumeConfigData> xcfgs = new HashMap<String, FlumeConfigData>();
    for (ConfigurationManager fcfg : flows.values()) {
      xcfgs.putAll(fcfg.getTranslatedConfigs());
    }
    return xcfgs;
  }

  /**
   * TODO (jon) this function should be extracted to a separate class and then
   * deprecated/removed.
   * 
   * TODO (jon) configs don't have a way of specifying a flow currently.
   * 
   * TODO (jon) this implementation is less than idea, should use bulk update
   */
  @Override
  synchronized public void loadConfigFile(String from) throws IOException {
    ConfigManager cfgs = new ConfigManager();
    cfgs.loadConfigFile(from);
    String defaultFlow = FlumeConfiguration.get().getDefaultFlowName();
    for (Entry<String, FlumeConfigData> e : cfgs.getAllConfigs().entrySet()) {
      FlumeConfigData fcd = e.getValue();
      // Flow names aren't saved with this mechanism.
      try {
        setConfig(e.getKey(), defaultFlow, fcd.sourceConfig, fcd.sinkConfig);
      } catch (FlumeSpecException e1) {
        throw new IOException(e1.getMessage(), e1);
      }
    }
    refreshAll();
  }

  @Override
  synchronized public void refresh(String logicalNode) throws IOException {
    String oldflow = getFlowId(logicalNode);
    parent.refresh(logicalNode);
    ConfigurationManager fcfg = getCreateFlowConfigMan(oldflow);
    FlumeConfigData fcd = parent.getConfig(logicalNode);
    try {
      fcfg.setConfig(logicalNode, fcd.flowID, fcd.sourceConfig, fcd.sinkConfig);
    } catch (FlumeSpecException e) {
      throw new IOException(e.getMessage(), e);
    }

  }

  @Override
  synchronized public void refreshAll() throws IOException {
    parent.refreshAll();
    for (Entry<String, FlumeConfigData> e : parent.getAllConfigs().entrySet()) {
      // TODO This is currently less than ideal -- should use bulk operation
      String node = e.getKey();
      refresh(node);
    }
  }

  @Override
  synchronized public void removeLogicalNode(String logicNode)
      throws IOException {
    String oldflow = getFlowId(logicNode);
    parent.removeLogicalNode(logicNode);
    ConfigurationManager flowCfg = flows.get(oldflow);
    if (flowCfg != null) {
      flowCfg.removeLogicalNode(logicNode);
    }
  }

  /**
   * TODO (jon) This should be extracted into a separate class and
   * deprecated/removed.
   */
  @Override
  synchronized public void saveConfigFile(String from) throws IOException {
    parent.saveConfigFile(from);
  }

  @Override
  synchronized public void setConfig(String host, String flowid, String source,
      String sink) throws IOException, FlumeSpecException {
    String oldflow = getFlowId(host);
    if (oldflow != null && !oldflow.equals(flowid)) {
      // this is a flow move, remove from old flow
      flows.get(oldflow).removeLogicalNode(host);
    }
    parent.setConfig(host, flowid, source, sink);
    getCreateFlowConfigMan(flowid).setConfig(host, flowid, source, sink);
  }

  @Override
  synchronized public void start() throws IOException {
    parent.start();
    for (ConfigurationManager fcfg : flows.values()) {
      fcfg.start();
    }
    refreshAll();
  }

  @Override
  synchronized public void stop() throws IOException {
    parent.stop();
    for (ConfigurationManager fcfg : flows.values()) {
      fcfg.stop();
    }
  }

  @Override
  synchronized public void unmapAllLogicalNodes() throws IOException {
    parent.unmapAllLogicalNodes();
    for (ConfigurationManager fcfg : flows.values()) {
      fcfg.unmapAllLogicalNodes();
    }
  }

  @Override
  synchronized public void unmapLogicalNode(String physNode, String logicNode) {
    String flow = getFlowId(logicNode);
    parent.unmapLogicalNode(physNode, logicNode);
    getCreateFlowConfigMan(flow).unmapLogicalNode(physNode, logicNode);
  }

  @Override
  synchronized public void updateAll() throws IOException {
    parent.updateAll();
    for (ConfigurationManager fcfg : flows.values()) {
      fcfg.updateAll();
    }
  }

  @Override
  public String getName() {
    return "FlowConfigManager";
  }

  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put("parent." + parent.getName(), parent);

    for (Entry<String, ConfigurationManager> e : flows.entrySet()) {
      map.put("flow[" + e.getKey() + "].", e.getValue());
    }
    return map;
  }

  /**
   * This creates a FailoverConfiguration manager with LogicalNode translations
   * that isolates each failover chain translation by flow id.
   */
  public static class FailoverFlowConfigManager extends FlowConfigManager {
    /**
     * This is needed when constructing child ConfigurationManagers
     */
    final StatusManager statman;

    public FailoverFlowConfigManager(ConfigurationManager parent,
        StatusManager statman) {
      super(parent);
      this.statman = statman;
    }

    public ConfigurationManager createConfigMan() {
      // Create with all memory based config managers
      return new FailoverConfigurationManager(new ConfigManager(),
          new ConfigManager(), new ConsistentHashFailoverChainManager(3));
    }
  };

}
