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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.FlumeClientServer;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeNodeState;
import com.cloudera.flume.conf.thrift.FlumeClientServer.Iface;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.server.FlumeReport;
import com.cloudera.flume.util.ThriftServer;
import com.google.common.base.Preconditions;

/**
 * Master-side implementation of the master<->client RPC interface over Thrift.
 */
public class MasterClientServer extends ThriftServer implements
    FlumeClientServer.Iface {
  Logger LOG = Logger.getLogger(MasterClientServer.class);
  final protected FlumeMaster master;
  final protected int port;

  public MasterClientServer(FlumeMaster master) {
    Preconditions.checkArgument(master != null,
        "FlumeConfigMaster is null in MasterClientServer!");
    this.master = master;
    this.port = FlumeConfiguration.get().getMasterHeartbeatPort();
  }

  @Override
  public List<String> getLogicalNodes(String physNode) throws TException {
    return master.getSpecMan().getLogicalNode(physNode);
  }

  @Override
  public FlumeConfigData getConfig(String host) throws TException {
    return master.getSpecMan().getConfig(host);
  }

  /**
   * Returns true if needs to do a update configuration Here host is the logical
   * node name. Version is the node's current configuration version.
   */
  @Override
  public boolean heartbeat(String logicalNode, String physicalNode,
      String clienthost, FlumeNodeState s, long version) throws TException {

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
        clienthost, physicalNode, logicalNode, stateFromThrift(s), version);
    FlumeConfigData cfg = master.getSpecMan().getConfig(logicalNode);

    if (cfg == null || version < cfg.getTimestamp()) {
      configChanged = true;
      // version sent by node is older than current, return true to force config
      // upgrade
    }
    return configChanged;

  }

  @Override
  public void acknowledge(String ackid) throws TException {
    master.getAckMan().acknowledge(ackid);
  }

  @Override
  public boolean checkAck(String ackid) throws TException {
    return master.getAckMan().check(ackid);
  }

  public void serve() throws TTransportException {
    LOG
        .info(String
            .format(
                "Starting blocking thread pool server for control server on port %d...",
                port));
    this.start(new FlumeClientServer.Processor((Iface) this), port,
        "MasterClientServer");
  }

  /**
   * Converts a thrift generated NodeStatus enum value to a flume master
   * StatusManager NodeState enum
   */
  public static NodeState stateFromThrift(FlumeNodeState s) {
    Preconditions.checkNotNull(s, "Argument may not be null.");
    switch (s) {
    case ACTIVE:
      return NodeState.ACTIVE;
    case CONFIGURING:
      return NodeState.CONFIGURING;
    case ERROR:
      return NodeState.ERROR;
    case HELLO:
      return NodeState.HELLO;
    case IDLE:
      return NodeState.IDLE;
    case LOST:
      return NodeState.LOST;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }

  /**
   * Converts a flume master StatusManager NodeState enum to a thrift generated
   * NodeStatus enum value.
   */
  public static FlumeNodeState stateToThrift(NodeState s) {
    Preconditions.checkNotNull(s, "Argument may not be null.");
    switch (s) {
    case ACTIVE:
      return FlumeNodeState.ACTIVE;
    case CONFIGURING:
      return FlumeNodeState.CONFIGURING;
    case ERROR:
      return FlumeNodeState.ERROR;
    case HELLO:
      return FlumeNodeState.HELLO;
    case IDLE:
      return FlumeNodeState.IDLE;
    case LOST:
      return FlumeNodeState.LOST;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }

  /**
   * Adds a set of reports to the singleton ReportManager, after wrapping them
   * in Reportable objects.
   */
  @Override
  public void putReports(Map<String, FlumeReport> reports) throws TException {
    Preconditions.checkNotNull(reports,
        "putReports called with null report map");
    ReportManager rptManager = ReportManager.get();
    for (final Entry<String, FlumeReport> r : reports.entrySet()) {
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
}
