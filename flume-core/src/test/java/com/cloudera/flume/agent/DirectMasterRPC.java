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

package com.cloudera.flume.agent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.NetUtils;

/**
 * This is a MasterRPC interface that actually just hooks into a Master
 * directly.
 * 
 * TODO (jon) Replace MockMaster with this.
 */
public class DirectMasterRPC implements MasterRPC {

  FlumeMaster master;
  String physicalNode;

  /**
   * This constructor builds a direct connector to the master as if it was a
   * physical node with name physicalName.
   */
  public DirectMasterRPC(FlumeMaster master, String physicalName) {
    this.master = master;
    this.physicalNode = physicalName;
  }

  public DirectMasterRPC(FlumeMaster master) {
    this(master, NetUtils.localhost());
  }

  @Override
  public void acknowledge(String group) throws IOException {
    master.getAckMan().acknowledge(group);
  }

  @Override
  public boolean checkAck(String ackid) throws IOException {
    return master.getAckMan().check(ackid);
  }

  public void open() throws IOException {

  }

  @Override
  public void close() {
    // Do nohting.
  }

  // TODO (jon) not sure this belongs here.
  @Override
  public AckListener createAckListener() {
    return new CollectorAckListener(this);
  }

  @Override
  public FlumeConfigData getConfig(String n) throws IOException {
    return master.getSpecMan().getConfig(n);
  }

  @Override
  public List<String> getLogicalNodes(String physNode) throws IOException {
    return new ArrayList<String>(master.getSpecMan().getLogicalNode(physNode));
  }

  @Override
  public boolean heartbeat(LogicalNode n) throws IOException {
    // TODO (jon) this is copy and pasted from MasterClientServer. this should
    // be refactored to share the code.
    String logicalNode = n.getName();
    long version = n.getConfigVersion();

    // sanity check with physicalnode.
    List<String> lns = master.getSpecMan().getLogicalNode(physicalNode);
    if (lns == null || !lns.contains(logicalNode)) {
      if (physicalNode.equals(logicalNode)) {
        // auto add a default logical node if there are no logical nodes for a
        // physical node.
        master.getSpecMan().addLogicalNode(physicalNode, logicalNode);
      }
      // LOG.warn("Recieved heartbeat from node '" + physicalNode + "/"
      // + logicalNode + "' that is not be set by master ");
    }

    boolean configChanged = master.getStatMan().updateHeartbeatStatus(
        NetUtils.localhost(), n.getStatus().physicalNode, logicalNode,
        n.getStatus().state, version);
    FlumeConfigData cfg = master.getSpecMan().getConfig(logicalNode);

    if (cfg == null || version < cfg.getTimestamp()) {
      configChanged = true;
      // version sent by node is older than current, return true to force
      // config
      // upgrade
    }
    return configChanged;

  }

  /**
   * Creates a reportable for each ReportEvent and adds it to the global
   * ReportManager
   */
  @Override
  public void putReports(Map<String, ReportEvent> reports) throws IOException {
    // Don't do anything. This call is for moving reports from a FlumeNode to
    // the master. But if they are using DirectMasterRPC then they're sharing
    // the same ReportManager and you don't want to overwrite existing
    // reportables.
  }

  @Override
  public HashMap<String, Integer> getChokeMap(String physNode)
      throws IOException {
    return new HashMap<String, Integer>(master.getSpecMan().getChokeMap(
        physNode));
  }

}
