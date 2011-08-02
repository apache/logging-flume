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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

/**
 * This maintains the global configuration state of the flume nodes.
 */
public class ConfigManager implements ConfigurationManager {
  static Logger LOG = Logger.getLogger(ConfigManager.class);
  ConfigStore cfgStore;
  Map<String, String> logicalToPhysical = new HashMap<String, String>();

  public ConfigManager(ConfigStore store) {
    cfgStore = store;
  }

  /**
   * Used for testing - defaults to a MemoryBackedConfigStore
   */
  public ConfigManager() {
    cfgStore = new MemoryBackedConfigStore();
  }

  synchronized public FlumeConfigData getConfig(String host) {
    return cfgStore.getConfig(host);
  }

  @Override
  public String getName() {
    return "configuration manager";
  }

  /**
   * This sets a specified configuration. Only valid source and sinks are
   * allowed. An exception is thrown if any piece is unparsable, or fails to
   * instantiate.
   */
  synchronized public void setConfig(String logicalNode, String flowid,
      String source, String sink) throws IOException {

    try {
      // make sure the sink specified is parsable and instantiable.
      FlumeBuilder.buildSink(new Context(), sink);
      FlumeBuilder.buildSource(source);
    } catch (Exception e) {
      throw new IOException("Attempted to write an invalid sink/source: "
          + e.getMessage(), e);
    }

    cfgStore.setConfig(logicalNode, flowid, source, sink);
  }

  /**
   * Sets many configurations in one operation. The change should be atomic. All
   * changes go in or none of them go in (and exception thrown).
   */
  synchronized public void setBulkConfig(Map<String, FlumeConfigData> configs)
      throws IOException {
    cfgStore.bulkSetConfig(configs);
  }

  /**
   * Returns a copy of the map of all configurations
   */
  synchronized public Map<String, FlumeConfigData> getAllConfigs() {
    return new HashMap<String, FlumeConfigData>(cfgStore.getConfigs());
  }

  /**
   * Returns a copy of the translations of all configurations
   */
  synchronized public Map<String, FlumeConfigData> getTranslatedConfigs() {
    // translated and non translated are the same on the StoreConfigManager
    return new HashMap<String, FlumeConfigData>(cfgStore.getConfigs());
  }

  /**
   * internal method to dump flume config data.
   */
  static void appendHtmlFlumeConfigData(StringBuilder html, String name,
      FlumeConfigData fcd) {
    html.append("\n<tr>");
    html.append("<td>" + name + "</td>");
    FlumeConfigData cfg = fcd;
    html.append("<td>" + new Date(cfg.timestamp) + "</td>");
    html.append("<td>" + cfg.sourceConfig + "</td>");
    html.append("<td>" + cfg.sinkConfig + "</td>");
    html.append("</tr>\n");
  }

  /**
   * internal method to dump logical physical mapping.
   */
  static void appendHtmlPhysicalLogicalMapping(StringBuilder html,
      String physical, Collection<String> logicals) {
    html.append("\n<tr>");
    html.append("<td>" + physical + "</td>");
    Collection<String> lns = logicals;
    html.append("<td>" + StringUtils.join(lns, ',') + "</td>");
    html.append("</tr>\n");
  }

  /**
   * Creates two tables for display in the primitive webapp.
   * 
   * TODO convert to a generic report
   */
  @Override
  synchronized public ReportEvent getReport() {
    StringBuilder html = new StringBuilder();
    html.append("<h2>Node configuration</h2>\n<table border=\"1\"><tr>"
        + "<th>Node</th><th>Version</th><th>Source</th><th>Sink</th></tr>");
    Map<String, FlumeConfigData> cfgs = cfgStore.getConfigs();
    synchronized (cfgs) {
      for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
        appendHtmlFlumeConfigData(html, e.getKey(), e.getValue());
      }
    }
    html.append("</table>\n\n");

    // a table that has a mapping from physical nodes to logical nodes.
    html.append("<h2>Physical/Logical Node mapping</h2>\n<table border=\"1\">"
        + "<tr><th>physical node</th><th>logical node</th></tr>");
    Multimap<String, String> nodes = cfgStore.getLogicalNodeMap();
    synchronized (nodes) {
      for (Entry<String, Collection<String>> e : nodes.asMap().entrySet()) {
        appendHtmlPhysicalLogicalMapping(html, e.getKey(), e.getValue());
      }
    }
    html.append("</table>\n\n");

    return ReportEvent.createLegacyHtmlReport("configs", html.toString());
  }

  /**
   * Loads configuration from a file. Synchronized to prevent races on
   * file/buffer allocate that could happen if saveConfig run concurrently.
   */
  synchronized public void loadConfigFile(String from) throws IOException {
    File f = new File(from);
    LOG.info("Loading configuration from: " + f.getAbsolutePath());
    FileInputStream r = null;
    try {
      r = new FileInputStream(f);
      long len = f.length();
      Preconditions.checkArgument(len <= Integer.MAX_VALUE);
      byte[] buf = new byte[(int) len];
      r.read(buf);
      String fullspec = new String(buf);
      List<FlumeNodeSpec> cfgs = FlumeSpecGen.generate(fullspec);
      for (FlumeNodeSpec spec : cfgs) {
        setConfig(spec.node, FlumeConfiguration.get().getDefaultFlowName(),
            spec.src, spec.sink);
      }
    } catch (FlumeSpecException e) {
      LOG.debug(e, e);
      throw new IOException(e.getMessage());
    } finally {
      if (r != null) {
        r.close();
      }
    }
  }

  /**
   * Saves all node configs to a file called s.
   * 
   * synchronized to prevent race if multiple saveConfigs run concurrently. Does
   * not protect potential race caused by external concurrent FS modifications.
   */
  synchronized public void saveConfigFile(String s) throws IOException {
    File targ = new File(s); // final destination
    LOG.info("Saving configuration to: " + targ.getAbsolutePath());

    File targ2 = new File(s + "~"); // backup file

    // write here before move/overwrite.
    File tmp = File.createTempFile("current-", ".flume", targ.getParentFile());

    // This function follows file name, not the file. (e.g. the tmp name will be
    // deleted on exit, the renamed will remain.).
    tmp.deleteOnExit();
    PrintWriter out = new PrintWriter(new FileWriter(tmp));

    // writh all specs to tmp.
    Map<String, FlumeConfigData> cfgs = cfgStore.getConfigs();
    for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
      String name = e.getKey();
      String snk = e.getValue().getSinkConfig();
      String src = e.getValue().getSourceConfig();
      out.println(FlumeBuilder.toLine(name, src, snk));
    }
    out.close();

    // remove old backup, move previous to backup, and write new copy to
    // current.
    if (!targ2.delete()) {
      // targ2 may not exist, so only warn here
      LOG.warn("Unable to delete config backup file: " + targ2);
    }
    if (targ.exists()) {
      // there can be a race here..

      // TODO (jon) Java 7 has atomic move functions, move to this once it is
      // adopted.

      // move old file to ~
      if (!targ.renameTo(targ2)) {
        LOG.warn("Unable to make backup of config file: " + targ + " to "
            + targ2);
      }
    }
    if (!tmp.renameTo(targ)) {
      throw new IOException("Unable to rename " + tmp + " to " + targ);
    }
  }

  @Override
  synchronized public List<String> getLogicalNode(String physNode) {
    return cfgStore.getLogicalNodes(physNode);
  }

  @Override
  synchronized public void addLogicalNode(String physNode, String logicNode) {
    cfgStore.addLogicalNode(physNode, logicNode);
    logicalToPhysical.put(logicNode, physNode);
  }

  /**
   * Removes a logical node from the logical to physical node mapping. This
   * should eventually cause a node to decommission the node.
   */
  @Override
  synchronized public void unmapLogicalNode(String physNode, String logicNode) {
    if (physNode.equals(logicNode)) {
      LOG.warn("Not allowed to unmap primary logical node from physical node");
      return; // just return here.
    }
    cfgStore.unmapLogicalNode(physNode, logicNode);
    logicalToPhysical.remove(logicNode);
  }

  @Override
  synchronized public String getPhysicalNode(String logicalNode) {
    return logicalToPhysical.get(logicalNode);
  }

  /**
   * This reads a configuration and the sets it again. This updates the version
   * stamp and forces nodes to update their configurations.
   */
  @Override
  synchronized public void refresh(String logicalNode) throws IOException {
    FlumeConfigData fcd = cfgStore.getConfig(logicalNode);
    if (fcd == null) {
      throw new IOException("Unable to refresh logicalNode " + logicalNode
          + ".  It doesn't exist!");
    }

    cfgStore.setConfig(logicalNode, fcd.getFlowID(), fcd.getSourceConfig(), fcd
        .getSinkConfig());
  }

  /**
   * This reads all configuration and the sets all of them again via a bulk
   * update. This updates the version stamp and forces nodes to update their
   * configurations.
   */
  @Override
  synchronized public void refreshAll() throws IOException {
    Map<String, FlumeConfigData> cfgs = new HashMap<String, FlumeConfigData>();
    for (Entry<String, FlumeConfigData> ent : getAllConfigs().entrySet()) {
      cfgs.put(ent.getKey(), ent.getValue());

    }
    setBulkConfig(cfgs);
  }

  /**
   * This removes the logical node data flow configuration from both the flow
   * table and the phys-logical mapping
   */
  @Override
  synchronized public void removeLogicalNode(String logicNode) {
    cfgStore.removeLogicalNode(logicNode);
    String physical = getPhysicalNode(logicNode);
    if (physical != null) {
      // could be possible if node is unmapped and then later removed
      cfgStore.unmapLogicalNode(physical, logicNode);
      logicalToPhysical.remove(logicNode);
    }
  }

  @Override
  synchronized public void start() throws IOException {
    Preconditions.checkNotNull(cfgStore, "Trying to stop null cfgStore");
    try {
      try {
        cfgStore.init();
      } catch (InterruptedException e) {
        // InterruptedException can be ignored in certain cases
        LOG.warn("ConfigStore was interrupted on startup, this may be ok", e);
      }
    } catch (IOException e) {
      LOG.error("ConfigStore init threw IOException", e);
      throw e;
    }
  }

  @Override
  synchronized public void stop() throws IOException {
    if (cfgStore == null) {
      LOG.warn("Trying to shutdown null cfgStore");
      return;
    }
    cfgStore.shutdown();
  }

  /**
   * Unmaps all logical nodes from phsyical nodes except for the default one.
   * (logical==physical)
   */
  @Override
  synchronized public void unmapAllLogicalNodes() throws IOException {
    cfgStore.unmapAllLogicalNodes();
    logicalToPhysical.clear();
    refreshAll();
  }

  @Override
  public void updateAll() throws IOException {
    // do nothing.
  }

  @Override
  public String toString() {
    return getAllConfigs().toString();
  }

  /**
   * Returns a copy of the logical node map.
   */
  @Override
  synchronized public Multimap<String, String> getLogicalNodeMap() {
    ListMultimap<String, String> map = ArrayListMultimap
        .<String, String> create(cfgStore.getLogicalNodeMap());
    return map;
  }
}
