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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;

/**
 * This translating configuration manager encapsulates the logic for having
 * multiple configuration translations. There are parent configuration and self
 * configuration manager. The parent may be a configuration manager that does
 * storage or another translating manager and is used to hold pre-translated
 * configurations. The self manager must be a storage configuration manager for
 * keeping translated configurations.
 * 
 * If in-place changes are ok, use the same configuration manager as the parent
 * and self. If not, use a different configuration manager for parent and self.
 * 
 * Read method calls on a TranslatingConfiguraitonManager always read from the
 * self manager. Write method calls write to the parent manager and write the
 * translated versions to the self manager.
 */
abstract public class TranslatingConfigurationManager implements
    ConfigurationManager, Translator {
  final static Logger LOG = Logger
      .getLogger(TranslatingConfigurationManager.class);
  ConfigurationManager parentMan;
  ConfigurationManager selfMan;

  /**
   * Create a new translator where the parent and the self are the same.
   */
  public TranslatingConfigurationManager(ConfigurationManager single) {
    this.parentMan = single;
    this.selfMan = single;
  }

  /**
   * Create a new translator with a parent and a self.
   */
  public TranslatingConfigurationManager(ConfigurationManager parent,
      ConfigurationManager self) {
    this.parentMan = parent;
    this.selfMan = self;
  }

  /**
   * {@inheritDoc}
   * 
   * Gets the translated configuration data.
   */
  @Override
  synchronized public FlumeConfigData getConfig(String host) {
    return selfMan.getConfig(host);
  }

  /**
   * {@inheritDoc} setConfig actually writes two entries -- the version which is
   * the user entered to the parent, and a translated version that a
   * heartbeating node would read into the self table.
   */
  @Override
  synchronized public void setConfig(String logicalnode, String flowid,
      String source, String sink) throws IOException, FlumeSpecException {

    // if parent and self are different, save original to parent.
    if (parentMan != selfMan) {
      parentMan.setConfig(logicalnode, flowid, source, sink);
      FlumeConfigData fcd = parentMan.getConfig(logicalnode);
      // parent may have translated
      source = fcd.getSourceConfig();
      sink = fcd.getSinkConfig();
    }

    String xsink = translateSink(logicalnode, sink);
    String xsource = translateSource(logicalnode, source);

    // save the translated sink that is sent to the master.
    selfMan.setConfig(logicalnode, flowid, xsource, xsink);
    updateAll();
  }

  @Override
  abstract public String getName();

  /**
   * Internal formatting method for FlumeConfigData and translated
   * FlumeConfigData
   */
  static void appendHtmlTranslatedFlumeConfigData(StringBuilder html,
      String name, FlumeConfigData fcd, FlumeConfigData xfcd) {
    html.append("\n<tr>");
    html.append("<td>" + name + "</td>");
    FlumeConfigData cfg = fcd;
    html.append("<td>" + new Date(cfg.timestamp) + "</td>");
    html.append("<td>" + cfg.sourceConfig + "</td>");
    html.append("<td>" + cfg.sinkConfig + "</td>");
    if (xfcd != null) {
      html.append("<td>" + new Date(xfcd.timestamp) + "</td>");
      html.append("<td>" + xfcd.sourceConfig + "</td>");
      html.append("<td>" + xfcd.sinkConfig + "</td>");
    } else {
      html.append("<td></td><td></td><td></td>");
    }

    html.append("</tr>\n");
  }

  /**
   * Builds a two html tables that display parent/self configs and logical node
   * mapping.
   * 
   * TODO convert to a report, do not depend on this output.
   */
  @Override
  synchronized public ReportEvent getReport() {
    StringBuilder html = new StringBuilder();
    html
        .append("<h2>Node configuration</h2>\n<table border=\"1\"><tr>"
            + "<th>Node</th><th>Version</th><th>Source</th><th>Sink</th>"
            + "<th>Translated Version</th><th>Translated Source</th><th>Translated Sink</th>"
            + "</tr>");
    Map<String, FlumeConfigData> cfgs = new TreeMap<String, FlumeConfigData>(
        parentMan.getAllConfigs());
    Map<String, FlumeConfigData> xcfgs = new TreeMap<String, FlumeConfigData>(
        selfMan.getTranslatedConfigs());

    for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
      String ln = e.getKey();
      appendHtmlTranslatedFlumeConfigData(html, e.getKey(), e.getValue(), xcfgs
          .get(ln));
    }
    html.append("</table>\n\n");

    // a table that has a mapping from physical nodes to logical nodes.
    html.append("<h2>Physical/Logical Node mapping</h2>\n<table border=\"1\">"
        + "<tr><th>physical node</th><th>logical node</th></tr>");
    Multimap<String, String> nodes = parentMan.getLogicalNodeMap();
    synchronized (nodes) {
      for (Entry<String, Collection<String>> e : nodes.asMap().entrySet()) {
        ConfigManager.appendHtmlPhysicalLogicalMapping(html, e.getKey(), e
            .getValue());
      }
    }
    html.append("</table>\n\n");

    return ReportEvent.createLegacyHtmlReport("configs", html.toString());
  }

  /**
   * {@inheritDoc} This always just forwards to the parent.
   */
  @Override
  synchronized public void loadConfigFile(String file) throws IOException {
    parentMan.loadConfigFile(file);
    refreshAll();
  }

  /**
   * {@inheritDoc} This always just forwards to the parent.
   */
  @Override
  synchronized public void saveConfigFile(String file) throws IOException {
    parentMan.saveConfigFile(file);
  }

  /**
   * Returns the self configurations.
   */
  @Override
  synchronized public Map<String, FlumeConfigData> getAllConfigs() {
    return parentMan.getAllConfigs();
  }

  /**
   * Returns the translations of all configuraitons
   */
  synchronized public Map<String, FlumeConfigData> getTranslatedConfigs() {
    return selfMan.getAllConfigs();
  }

  /**
   * This reads a configuration and the sets it again. This updates the version
   * stamp and forces nodes to update their configurations.
   * 
   * Since this manager intercepts the logical node configuration and writes the
   * user specified node to a different value, we actually read the user
   * specified source-sink pair and then use this manager's setConfig method to
   * include the autogenerated source-sink pair.
   */
  synchronized public void refresh(String logicalNode) throws IOException {
    FlumeConfigData fcd = parentMan.getConfig(logicalNode);
    if (fcd == null) {
      throw new IOException("original " + logicalNode + " not found");
    }
    try {
      setConfig(logicalNode, fcd.getFlowID(), fcd.getSourceConfig(), fcd
          .getSinkConfig());
    } catch (FlumeSpecException e) {
      throw new IOException(e);
    }
  }

  /**
   * This reads a configuration and updates the version stamp only if the new
   * configuration is different from the previous configuration.
   */
  synchronized public void updateAll() throws IOException {
    parentMan.updateAll();
    Map<String, FlumeConfigData> updates = new HashMap<String, FlumeConfigData>();
    for (Entry<String, FlumeConfigData> ent : parentMan.getTranslatedConfigs()
        .entrySet()) {
      String node = ent.getKey();

      // get the original name
      FlumeConfigData fcd = ent.getValue();
      String src = fcd.getSourceConfig();
      String snk = fcd.getSinkConfig();

      String xsnk, xsrc;
      try {
        xsnk = translateSink(node, snk);
        xsrc = translateSource(node, src);

        FlumeConfigData selfData = selfMan.getConfig(node);

        if (selfData != null && xsnk.equals(selfData.getSinkConfig())
            && xsrc.equals(selfData.getSourceConfig())) {
          // same as before? do nothing
          LOG.debug("xsnk==snk = " + xsnk);
          LOG.debug("xsrc==src = " + xsrc);
          continue;
        }
        FlumeConfigData xfcd = new FlumeConfigData(fcd);
        xfcd.setSourceConfig(xsrc);
        xfcd.setSinkConfig(xsnk);
        updates.put(node, xfcd);
      } catch (FlumeSpecException e) {
        LOG.error("Internal Error: " + e.getLocalizedMessage(), e);
        throw new IOException("Internal Error: " + e.getMessage());
      }
    }
    selfMan.setBulkConfig(updates);
  }

  /**
   * This reads a configuration and the sets it again. This updates the version
   * stamp and forces nodes to update their configurations.
   * 
   * Since this manager intercepts the logical node configuration and writes the
   * user specified node to a different value, we actually read the user
   * specified source-sink pair and then use this manager's setConfig method to
   * include the autogenerated source-sink pair.
   */
  @Override
  synchronized public void refreshAll() throws IOException {
    parentMan.refreshAll();
    Map<String, FlumeConfigData> updates = new HashMap<String, FlumeConfigData>();
    for (Entry<String, FlumeConfigData> ent : parentMan.getTranslatedConfigs()
        .entrySet()) {
      String node = ent.getKey();

      // get the original name
      FlumeConfigData fcd = ent.getValue();
      String src = fcd.getSourceConfig();
      String snk = fcd.getSinkConfig();

      String xsnk, xsrc;
      try {
        xsnk = translateSink(node, snk);
        xsrc = translateSource(node, src);
        FlumeConfigData xfcd = new FlumeConfigData(fcd);
        xfcd.setSinkConfig(xsnk);
        xfcd.setSourceConfig(xsrc);
        updates.put(node, xfcd);
      } catch (FlumeSpecException e) {
        LOG.error("Internal Error: " + e.getLocalizedMessage(), e);
        throw new IOException("Internal Error: " + e.getMessage());
      }

    }

    selfMan.setBulkConfig(updates);
  }

  /**
   * Updates both the parent and self managers with the set of configurations.
   */
  @Override
  synchronized public void setBulkConfig(Map<String, FlumeConfigData> configs)
      throws IOException {

    Map<String, FlumeConfigData> updates = new HashMap<String, FlumeConfigData>();
    Map<String, FlumeConfigData> selfupdates = new HashMap<String, FlumeConfigData>();
    for (Entry<String, FlumeConfigData> ent : configs.entrySet()) {
      String node = ent.getKey();

      // get the original name
      String src = ent.getValue().getSourceConfig();
      String snk = ent.getValue().getSinkConfig();

      String xsnk, xsrc;
      try {
        xsnk = translateSink(node, snk);
        xsrc = translateSource(node, src);
        if (selfMan != parentMan) {
          FlumeConfigData fcd = new FlumeConfigData(ent.getValue());
          updates.put(node, fcd);
        }
        FlumeConfigData xfcd = new FlumeConfigData(ent.getValue());
        xfcd.setSinkConfig(xsnk);
        xfcd.setSourceConfig(xsrc);
        selfupdates.put(node, xfcd);
      } catch (FlumeSpecException e) {
        LOG.error("Internal Error: " + e.getLocalizedMessage(), e);
        throw new IOException("Internal Error: " + e.getMessage());
      }
    }
    parentMan.setBulkConfig(updates);
    selfMan.setBulkConfig(selfupdates);
  }

  /**
   * Remove the logical node.
   */
  @Override
  synchronized public void removeLogicalNode(String logicNode) {
    // only remove once if parent == self
    if (parentMan != selfMan) {
      parentMan.removeLogicalNode(logicNode);
    }
    selfMan.removeLogicalNode(logicNode);
    try {
      updateAll();
    } catch (IOException e) {
      LOG.error("Error when removing logical node " + logicNode, e);
    }
  }

  /**
   * Start the sub managers.
   */
  @Override
  synchronized public void start() throws IOException {
    // if parent == self, only start once.
    Preconditions.checkNotNull(this.parentMan,
        "Trying to start with null cfgMan");
    if (parentMan != selfMan) {
      parentMan.start();
    }
    selfMan.start();
    updateAll();
  }

  /**
   * Stop the sub managers.
   */
  @Override
  synchronized public void stop() throws IOException {
    // if parent == self, only stop once.
    Preconditions.checkNotNull(this.parentMan,
        "Trying to stop with null cfgMan");
    if (parentMan != selfMan) {
      parentMan.stop();
    }
    selfMan.stop();
  }

  // //////////////////////////////////////////////////////////////////////////////
  // TODO decouple mapping from logical node configuration. Currently,
  // only forward node mappings calls to parent.

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public Multimap<String, String> getLogicalNodeMap() {
    return parentMan.getLogicalNodeMap();
  }

  /**
   * {@inheritDoc}
   */
  synchronized public List<String> getLogicalNode(String physNode) {
    return parentMan.getLogicalNode(physNode);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void addLogicalNode(String physNode, String logicNode) {
    parentMan.addLogicalNode(physNode, logicNode);
    try {
      updateAll();
    } catch (IOException e) {
      LOG.error("Error when mapping logical->physical node" + logicNode + "->"
          + physNode, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public String getPhysicalNode(String logicalNode) {
    return parentMan.getPhysicalNode(logicalNode);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void unmapLogicalNode(String physNode, String logicNode) {
    parentMan.unmapLogicalNode(physNode, logicNode);
    try {
      updateAll();
    } catch (IOException e) {
      LOG.error("Error when unmapping logical node " + e.getMessage(), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void unmapAllLogicalNodes() throws IOException {
    // mapping is only on parent.
    parentMan.unmapAllLogicalNodes();
    try {
      updateAll();
    } catch (IOException e) {
      LOG.error("Error when unmapping all logical nodes" + e.getMessage(), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return getTranslatedConfigs().toString();
  }
}
