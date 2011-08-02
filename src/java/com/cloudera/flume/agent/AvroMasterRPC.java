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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeReportAvro;
import com.cloudera.flume.conf.avro.FlumeReportAvroServer;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.master.MasterClientServerAvro;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.server.ReportServer;
import com.google.common.base.Preconditions;

/**
 * Avro implementation of a SinlgeMasterRPC. This manages the connection to an
 * Avro server and manages type translation.
 * 
 */
public class AvroMasterRPC implements MasterRPC {
  final static Logger LOG = Logger.getLogger(AvroMasterRPC.class);

  /**
   * Network name of the master
   */
  protected String masterHostname;

  /**
   * Master's heartbeat TCP port
   */
  protected int masterPort;

  /**
   * Avro RPC masterClient.
   */
  protected FlumeReportAvroServer masterClient;

  /**
   * Avro HTTP Transceiver.
   */
  protected HttpTransceiver trans;

  /**
   * Create an AvroMasterRPC that speaks to Avro server on
   * masterHost:masterPort.
   */
  AvroMasterRPC(String masterHost, int masterPort) throws IOException {
    this.masterHostname = masterHost;
    this.masterPort = masterPort;
    URL url = new URL("http", masterHostname, masterPort, "/");
    trans = new HttpTransceiver(url);
    this.masterClient = (FlumeReportAvroServer) SpecificRequestor.getClient(
        FlumeReportAvroServer.class, trans);
    LOG.info("Connected to master at " + masterHostname + ":" + masterPort);
  }

  public void close() {
    if (trans == null) {
      LOG.debug("Double close of Flume Node");
      return;
    }
    try {
      trans.close();
    } catch (IOException e) {
      LOG.warn("Could not close Avro HTTP transceiver open to port "
          + masterPort);
    }
    this.trans = null;
    this.masterClient = null;
  }

  public AckListener createAckListener() {
    Preconditions.checkNotNull(masterClient);
    return new CollectorAckListener(this);
  }

  private void ensureInitialized() throws IOException {
    if (this.masterClient == null || this.trans == null) {
      throw new IOException("MasterRPC called while not connected to master");
    }
  }

  public synchronized List<String> getLogicalNodes(String physNode)
      throws IOException {
    try {
      ensureInitialized();
      List<CharSequence> res = masterClient.getLogicalNodes(physNode);
      ArrayList<String> out = new ArrayList<String>((int) res.size());
      for (CharSequence r : res) {
        out.add(r.toString());
      }
      return out;
    } catch (AvroRemoteException e) {
      LOG.debug("RPC error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  public synchronized FlumeConfigData getConfig(LogicalNode n)
      throws IOException {
    try {
      ensureInitialized();
      AvroFlumeConfigData config = masterClient.getConfig(n.getName());
      if (config == null) {
        // master has not config for node
        LOG.debug("Master does not have config data for me.");
        return null;
      }
      return MasterClientServerAvro.configFromAvro(config);
    } catch (AvroRemoteException e) {
      LOG.debug("Avro error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public synchronized boolean checkAck(String ackid) throws IOException {
    try {
      ensureInitialized();
      return masterClient.checkAck(ackid);
    } catch (AvroRemoteException e) {
      LOG.debug("Avro error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public synchronized boolean heartbeat(LogicalNode n) throws IOException {
    try {
      ensureInitialized();
      NodeStatus status = n.getStatus();
      return masterClient.heartbeat(n.getName(), status.physicalNode,
          status.host, MasterClientServerAvro.stateToAvro(status.state), n
              .getConfigVersion());
    } catch (AvroRemoteException e) {
      LOG.debug("Avro error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public synchronized void acknowledge(String group) throws IOException {
    try {
      ensureInitialized();
      masterClient.acknowledge(group);
    } catch (AvroRemoteException e) {
      LOG.debug("Avro error on " + toString(), e);
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String toString() {
    return "Avro Master RPC to " + masterHostname + ":" + masterPort;
  }

  @Override
  public synchronized void putReports(Map<String, ReportEvent> reports)
      throws IOException {
    try {
      ensureInitialized();
      Map<CharSequence, FlumeReportAvro> flumeReports = new HashMap<CharSequence, FlumeReportAvro>();
      for (Entry<String, ReportEvent> e : reports.entrySet()) {
        flumeReports.put(e.getKey(), ReportServer.reportToAvro(e.getValue()));
      }
      masterClient.putReports(flumeReports);
    } catch (AvroRemoteException e) {
      LOG.debug("Avro error on" + toString(), e);
      throw new IOException("Avro Error", e);
    }
  }

}
