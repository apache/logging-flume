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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.FlumeClientServer;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeClientServer.Client;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.master.MasterClientServer;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.server.FlumeReport;
import com.cloudera.flume.reporter.server.ReportServer;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This class instantations node/master rpc comms using a thrift rpc mechanism.
 */
public class ThriftMasterRPC implements MasterRPC {

  final static Logger LOG = Logger.getLogger(ThriftMasterRPC.class.getName());

  // master config and connections
  final String masterHostname; // network name of the master
  final int masterPort; // master's heartbeat tcp port
  protected FlumeClientServer.Iface masterClient;// master thrift rpc client

  ThriftMasterRPC(FlumeConfiguration conf) {
    Preconditions
        .checkArgument(conf.getMasterHeartbeatServersList().size() > 0);
    // Rather than use two separate configurations for many masters and just one
    // we default to taking the first master in the list, and simply don't fail
    // over.
    Pair<String, Integer> master = conf.getMasterHeartbeatServersList().get(0);
    masterHostname = master.getLeft();
    masterPort = master.getRight();
  }

  public void open() throws IOException {
    try {
      open(masterHostname, masterPort);
    } catch (TTransportException e) {
      throw new IOException(e.getMessage());
    }
  }

  protected synchronized FlumeClientServer.Iface open(String host, int port)
      throws IOException, TTransportException {
    // single open only
    Preconditions.checkState(masterClient == null, // && masterTransport ==
        // null,
        "client already open -- double open not allowed");
    TTransport masterTransport = new TSocket(host, port);
    TProtocol protocol = new TBinaryProtocol(masterTransport);
    masterTransport.open();
    masterClient = new Client(protocol);
    LOG.info("Connected to master at " + host + ":" + port);
    return masterClient;
  }

  protected synchronized FlumeClientServer.Iface ensureConnected()
      throws TTransportException, IOException {
    return (masterClient != null) ? masterClient : open(masterHostname,
        masterPort);
  }

  public synchronized void close() {
    // multiple close is ok.
    if (masterClient != null) {
      TTransport masterTransport = ((FlumeClientServer.Client) masterClient)
          .getOutputProtocol().getTransport();
      masterTransport.close();
      LOG.info("Connection from node to master closed");
    } else {
      LOG.debug("double close of flume node");
    }

    masterClient = null;
  }

  /**
   * This is a hook to allow acks to be sent to the master. This generally will
   * happen from a collector node.
   */
  public AckListener createAckListener() {
    Preconditions.checkNotNull(masterClient);
    return new CollectorAckListener(this);
  }

  public synchronized List<String> getLogicalNodes(String physNode)
      throws IOException {
    try {
      ensureConnected();
      return masterClient.getLogicalNodes(physNode);
    } catch (TException e) {
      LOG.debug("RPC error on " + toString(), e);
      throw new IOException(e.getMessage());
    }

  }

  public synchronized FlumeConfigData getConfig(LogicalNode n)
      throws IOException {
    try {
      ensureConnected();
      return masterClient.getConfig(n.getName());
    } catch (TApplicationException e) {
      LOG.debug(e.getMessage()); // master has not config for node
      return null;
    } catch (TException e) {
      LOG.debug("Thrift error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  /**
   * This checks for an ack with a given ackid at the master
   */
  public synchronized boolean checkAck(String ackid) throws IOException {
    try {
      ensureConnected();
      return masterClient.checkAck(ackid);
    } catch (TException e) {
      LOG.debug("Thrift error on " + toString(), e);
      throw new IOException(e.getMessage());
    }

  }

  public synchronized boolean heartbeat(LogicalNode n) throws IOException {
    try {
      ensureConnected();
      NodeStatus status = n.getStatus();
      return masterClient.heartbeat(n.getName(), status.physicalNode,
          status.host, MasterClientServer.stateToThrift(status.state), n
              .getConfigVersion());

    } catch (TException e) {
      LOG.debug("Thrift error on " + toString(), e);
      throw new IOException(e.getMessage());
    }

  }

  @Override
  public synchronized void acknowledge(String group) throws IOException {
    try {
      ensureConnected();
      masterClient.acknowledge(group);
    } catch (TException e) {
      LOG.debug("Thrift error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  public String toString() {
    return "Thrift Master RPC to " + masterHostname + ":" + masterPort;
  }

  @Override
  public synchronized void putReports(Map<String, ReportEvent> reports)
      throws IOException {
    try {
      ensureConnected();
      Map<String, FlumeReport> flumeReports = new HashMap<String, FlumeReport>();
      for (Entry<String, ReportEvent> e : reports.entrySet()) {
        flumeReports.put(e.getKey(), ReportServer.reportToThrift(e.getValue()));
      }
      masterClient.putReports(flumeReports);
    } catch (TException e) {
      LOG.debug("Thrift error on" + toString(), e);
      throw new IOException("Thrift Error", e);
    }
  }
}
