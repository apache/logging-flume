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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.specific.SpecificResponder;
import org.apache.log4j.Logger;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;

import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeNodeState;
import com.cloudera.flume.conf.avro.FlumeReportAvro;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.avro.FlumeReportAvroServer;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportEvent;

import com.google.common.base.Preconditions;

/**
 * Avro implementation of a Master server. Performs type conversion and
 * delegates to a MasterClientServer.
 */
public class MasterClientServerAvro implements FlumeReportAvroServer, RPCServer {
  Logger LOG = Logger.getLogger(MasterClientServer.class);
  final protected int port;
  protected MasterClientServer delegate;
  protected Server server;

  public MasterClientServerAvro(MasterClientServer delegate) {
    Preconditions.checkArgument(delegate != null,
        "MasterCleintServer is null in 'AvroMasterClientServer!");
    this.delegate = delegate;
    this.port = FlumeConfiguration.get().getMasterHeartbeatPort();
  }

  public List<CharSequence> getLogicalNodes(CharSequence physNode)
      throws AvroRemoteException {
    List<String> str = delegate.getLogicalNodes(physNode.toString());
    List<CharSequence> out = new ArrayList<CharSequence>();
    for (String s : str) {
      out.add(s);
    }
    return out;
  }

  public AvroFlumeConfigData getConfig(CharSequence host)
      throws AvroRemoteException {
    FlumeConfigData data = delegate.getConfig(host.toString());
    if (data != null) {
      return configToAvro(data);
    }
    return null;
  }

  /**
   * Returns true if needs to do a update configuration Here host is the logical
   * node name. Version is the node's current configuration version.
   */
  public boolean heartbeat(CharSequence logicalNode, CharSequence physicalNode,
      CharSequence clienthost, FlumeNodeState s, long version)
      throws AvroRemoteException {

    return delegate.heartbeat(logicalNode.toString(), physicalNode.toString(),
        clienthost.toString(), stateFromAvro(s), version);
  }

  public java.lang.Void acknowledge(CharSequence ackid)
      throws AvroRemoteException {
    delegate.acknowledge(ackid.toString());
    return null;
  }

  public boolean checkAck(CharSequence ackid) throws AvroRemoteException {
    return delegate.checkAck(ackid.toString());
  }

  public java.lang.Void putReports(Map<CharSequence, FlumeReportAvro> reports)
      throws AvroRemoteException {
    Preconditions.checkNotNull(reports,
        "putReports called with null report map");
    Map<String, ReportEvent> reportsMap = new HashMap<String, ReportEvent>();
    for (Entry<CharSequence, FlumeReportAvro> r : reports.entrySet()) {
      Map<String, Long> longMetrics = new HashMap<String, Long>();
      Map<String, Double> doubleMetrics = new HashMap<String, Double>();
      Map<String, String> stringMetrics = new HashMap<String, String>();
      for (CharSequence key : r.getValue().longMetrics.keySet()) {
        longMetrics.put(key.toString(), r.getValue().longMetrics.get(key));
      }
      for (CharSequence key : r.getValue().stringMetrics.keySet()) {
        stringMetrics.put(key.toString(), r.getValue().stringMetrics.get(key)
            .toString());
      }
      for (CharSequence key : r.getValue().doubleMetrics.keySet()) {
        doubleMetrics.put(key.toString(), r.getValue().doubleMetrics.get(key));
      }
      reportsMap.put(r.getKey().toString(), new ReportEvent(longMetrics,
          stringMetrics, doubleMetrics));
    }
    delegate.putReports(reportsMap);
    return null;
  }

  // CONTROL
  public void serve() throws IOException {
    LOG
        .info(String
            .format(
                "Starting blocking thread pool server for control server on port %d...",
                port));
    SpecificResponder res = new SpecificResponder(FlumeReportAvroServer.class,
        this);
    this.server = new HttpServer(res, port);
    this.server.start();
  }

  public void stop() {
    LOG.info(String.format("Stopping control server on port %d...", port));
    this.server.close();
  }

  // TYPE CONVERSION
  /**
   * Converts a Avro generated NodeStatus enum value to a flume master
   * StatusManager NodeState enum
   */
  public static NodeState stateFromAvro(FlumeNodeState s) {
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
    case DECOMMISSIONED:
      return NodeState.DECOMMISSIONED;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }

  /**
   * Converts a flume master StatusManager NodeState enum to a Avro generated
   * NodeStatus enum value.
   */
  public static FlumeNodeState stateToAvro(NodeState s) {
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
    case DECOMMISSIONED:
      return FlumeNodeState.DECOMMISSIONED;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }

  public static AvroFlumeConfigData configToAvro(FlumeConfigData in) {
    if (in == null) {
      return null;
    }
    AvroFlumeConfigData out = new AvroFlumeConfigData();
    out.timestamp = in.timestamp;
    out.sourceConfig = in.sourceConfig;
    out.sinkConfig = in.sinkConfig;
    out.sourceVersion = in.sourceVersion;
    out.sinkVersion = in.sinkVersion;
    out.flowID = in.flowID;
    return out;
  }

  public static FlumeConfigData configFromAvro(AvroFlumeConfigData in) {
    if (in == null) {
      return null;
    }
    FlumeConfigData out = new FlumeConfigData();
    out.timestamp = in.timestamp;
    out.sourceConfig = in.sourceConfig.toString();
    out.sinkConfig = in.sinkConfig.toString();
    out.sourceVersion = in.sourceVersion;
    out.sinkVersion = in.sinkVersion;
    out.flowID = in.flowID.toString();
    return out;
  }

  @Override
  public Map<CharSequence, Integer> getChokeMap(CharSequence physNode)
      throws AvroRemoteException {
    Map<String, Integer> chokeMap = delegate.getChokeMap(physNode.toString());
    Map<CharSequence, Integer> newMap = new HashMap<CharSequence, Integer>();
    for (String s : chokeMap.keySet()) {
      newMap.put(s, chokeMap.get(s));
    }
    return newMap;
  }
}
