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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.ThriftFlumeClientServer;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeNodeState;
import com.cloudera.flume.conf.thrift.ThriftFlumeClientServer.Iface;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.server.thrift.ThriftFlumeReport;
import com.cloudera.flume.util.ThriftServer;
import com.google.common.base.Preconditions;

/**
 * Thrift implementation of a Master server. Performs type conversion and
 * delegates to a MasterClientServer.
 */
public class MasterClientServerThrift extends ThriftServer implements
    ThriftFlumeClientServer.Iface, RPCServer {
  static final Logger LOG = LoggerFactory.getLogger(MasterClientServerThrift.class);
  final protected int port;
  protected MasterClientServer delegate;

  public MasterClientServerThrift(MasterClientServer delegate) {
    Preconditions.checkArgument(delegate != null,
        "MasterClientServer is null in 'MasterClientServerThrift!");
    this.delegate = delegate;
    this.port = FlumeConfiguration.get().getMasterHeartbeatPort();
  }

  public List<String> getLogicalNodes(String physNode) throws TException {
    return delegate.getLogicalNodes(physNode);
  }

  public Map<String, Integer> getChokeMap(String physNode) throws TException {
    return delegate.getChokeMap(physNode);
  }

  public ThriftFlumeConfigData getConfig(String host) throws TException {
    return configToThrift(delegate.getConfig(host));
  }

  public boolean heartbeat(String logicalNode, String physicalNode,
      String clienthost, FlumeNodeState s, long version) throws TException {
    return delegate.heartbeat(logicalNode, physicalNode, clienthost,
        stateFromThrift(s), version);
  }

  public void acknowledge(String ackid) throws TException {
    delegate.acknowledge(ackid);
  }

  public boolean checkAck(String ackid) throws TException {
    return delegate.checkAck(ackid);
  }

  public void putReports(Map<String, ThriftFlumeReport> reports) throws TException {
    Preconditions.checkNotNull(reports,
        "putReports called with null report map");
    Map<String, ReportEvent> reportsMap = new HashMap<String, ReportEvent>();
    for (final Entry<String, ThriftFlumeReport> r : reports.entrySet()) {
      ReportEvent event = new ReportEvent(r.getValue().longMetrics, r
          .getValue().stringMetrics, r.getValue().doubleMetrics);
      reportsMap.put(r.getKey(), event);
    }
    delegate.putReports(reportsMap);
  }

  // CONTROL
  public void serve() throws IOException {
    LOG
        .info(String
            .format(
                "Starting blocking thread pool server for control server on port %d...",
                port));
    try {
      this.start(new ThriftFlumeClientServer.Processor((Iface) this), port,
          "MasterClientServer");
    } catch (TTransportException e) {
      throw new IOException(e.getMessage());
    }
  }

  // TYPE CONVERSION
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
      return NodeState.OPENING;
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
    case CLOSING:
      return NodeState.CLOSING;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }

  public static FlumeConfigData configFromThrift(ThriftFlumeConfigData in) {
    if (in == null) {
      return null;
    }
    FlumeConfigData out = new FlumeConfigData();
    out.timestamp = in.timestamp;
    out.sourceConfig = in.sourceConfig;
    out.sinkConfig = in.sinkConfig;
    out.sourceVersion = in.sourceVersion;
    out.sinkVersion = in.sinkVersion;
    out.flowID = in.flowID;
    return out;
  }

  public static ThriftFlumeConfigData configToThrift(FlumeConfigData in) {
    if (in == null) {
      return null;
    }
    ThriftFlumeConfigData out = new ThriftFlumeConfigData();
    out.timestamp = in.timestamp;
    out.sourceConfig = in.sourceConfig;
    out.sinkConfig = in.sinkConfig;
    out.sourceVersion = in.sourceVersion;
    out.sinkVersion = in.sinkVersion;
    out.flowID = in.flowID;
    return out;
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
    case OPENING:
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
    case CLOSING:
      return FlumeNodeState.CLOSING;
    default:
      throw new IllegalStateException("Unknown value " + s);
    }
  }
}
