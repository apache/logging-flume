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
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * Master-side implementation of the master<->client RPC interaction.
 * Encapsulates the logic involved in processing client requests. Since the 
 * wire protocol is different for separate RPC implementations, they each 
 * run their own stub servers, then delegate all requests to this common class.
 */
public class MasterClientServer {
  Logger LOG = Logger.getLogger(MasterClientServer.class);
  final protected FlumeMaster master;
  final protected FlumeConfiguration config;
  
  RPCServer masterRPC;

  public MasterClientServer(FlumeMaster master, FlumeConfiguration config) 
    throws IOException {
    Preconditions.checkArgument(master != null,
        "FlumeConfigMaster is null in MasterClientServer!");
    this.master = master;
    this.config = config;
    String rpcType = config.getMasterHeartbeatRPC();
    masterRPC = null;
    if (FlumeConfiguration.RPC_TYPE_AVRO.equals(rpcType)) {
      masterRPC = new MasterClientServerAvro(this);
    } else if (FlumeConfiguration.RPC_TYPE_THRIFT.equals(rpcType)) {
      masterRPC = new MasterClientServerThrift(this);
    } else {
      throw new IOException("No valid RPC framework specified in config");
    }
  }
  
  public MasterClientServer(FlumeMaster master, FlumeConfiguration config, 
      RPCServer rpc) {
    this.master = master;
    this.config = config;
    this.masterRPC = rpc;
  }
  
  /**
   * For testing.
   */
  public RPCServer getMasterRPC() {
    return this.masterRPC;
  }

  public List<String> getLogicalNodes(String physNode) {
    return master.getSpecMan().getLogicalNode(physNode);
  }

  public FlumeConfigData getConfig(String host) {
    FlumeConfigData config = master.getSpecMan().getConfig(host);
    if (config != null) {
      return config;
    }
    return null;
  }

  /**
   * Returns true if needs to do a update configuration Here host is the logical
   * node name. Version is the node's current configuration version.
   */
  public boolean heartbeat(String logicalNode, String physicalNode,
      String clienthost, NodeState s, long version) {

    // sanity check with physicalnode.
    List<String> lns = master.getSpecMan().getLogicalNode(physicalNode);
    if (lns == null || !lns.contains(logicalNode)) {
      if (physicalNode.equals(logicalNode)) {
        // auto add a default logical node if there are no logical nodes for a
        // physical node.
        master.getSpecMan().addLogicalNode(physicalNode, logicalNode);
      }
      LOG.warn("Recieved heartbeat from node '" + physicalNode + "/"
          + logicalNode + "' that is not be set by master ");
    }

    boolean configChanged = master.getStatMan().updateHeartbeatStatus(
        clienthost, physicalNode, logicalNode, s, version);
    FlumeConfigData cfg = master.getSpecMan().getConfig(logicalNode);

    if (cfg == null || version < cfg.getTimestamp()) {
      configChanged = true;
      // version sent by node is older than current, return true to force config
      // upgrade
    }
    return configChanged;
  }

  public void acknowledge(String ackid) {
    master.getAckMan().acknowledge(ackid);
  }

  public boolean checkAck(String ackid) {
    return master.getAckMan().check(ackid);
  }

  /**
   * Adds a set of reports to the singleton ReportManager, after wrapping them
   * in Reportable objects.
   */
  public void putReports(Map<String, ReportEvent> reports) {
    Preconditions.checkNotNull(reports,
        "putReports called with null report map");
    ReportManager rptManager = ReportManager.get();
    for (final Entry<String, ReportEvent> r : reports.entrySet()) {
      rptManager.add(new Reportable() {

        @Override
        public String getName() {
          return r.getKey();
        }

        @Override
        public ReportEvent getReport() {
          return new ReportEvent(r.getValue());
        }
      });
    }
  }

  public void serve() throws IOException {
    this.masterRPC.serve();
  }
  
  public void stop () throws IOException {
    if (this.masterRPC != null) {
      this.masterRPC.stop();
    }
  }
}
