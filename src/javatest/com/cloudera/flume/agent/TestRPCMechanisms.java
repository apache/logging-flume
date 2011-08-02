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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.specific.SpecificResponder;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.avro.util.Utf8;
import org.mortbay.log.Log;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeReportAvro;
import com.cloudera.flume.conf.avro.FlumeReportAvroServer;
import com.cloudera.flume.conf.avro.FlumeNodeState;
import com.cloudera.flume.conf.thrift.FlumeClientServer;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeClientServer.Iface;
import com.cloudera.flume.master.MasterClientRPC;
import com.cloudera.flume.master.MasterClientServerAvro;
import com.cloudera.flume.master.MasterClientServerThrift;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.server.FlumeReport;
import com.cloudera.flume.util.ThriftServer;
/**
 * Test that both Thrift and Avro RPC type translation is working. Also 
 * tests basic stub client/server interaction.
 */
public class TestRPCMechanisms {
  Logger LOG = Logger.getLogger(TestThriftMultiMasterRPC.class);

  /**
   * Mock AvroServer.
   */
  public class MockAvroServer implements FlumeReportAvroServer {
    protected Server server;

    public MockAvroServer() {
      
    }
    
    public void stop() {
      this.server.close();
    }
    
    public void serve(int port) throws IOException {
      LOG.info(String
        .format(
        "Starting blocking thread pool server for control server on port %d...",
        port));
      SpecificResponder res = new SpecificResponder(
          FlumeReportAvroServer.class, this);
      this.server = new HttpServer(res, port);
    }

    @Override
    public java.lang.Void acknowledge(Utf8 ackid) throws AvroRemoteException {
      return null;
    }

    @Override
    public boolean checkAck(Utf8 ackid) throws AvroRemoteException {
      Log.info("checkAck called at server on " + this.server.getPort());
      return true;
    }

    @Override
    public AvroFlumeConfigData getConfig(Utf8 sourceId) 
      throws AvroRemoteException {
      Log.info("getConfig called at server on " + this.server.getPort());
      FlumeConfigData out = new FlumeConfigData();
      out.flowID = "flowID";
      out.sinkConfig = "sinkConfig";
      out.sinkVersion = 112233;
      out.sourceConfig = "sourceConfig";
      out.sourceVersion = 445566;
      out.timestamp = 78901;
      return MasterClientServerAvro.configToAvro(out);
    }

    @Override
    public GenericArray<Utf8> getLogicalNodes(Utf8 physNode) 
      throws AvroRemoteException {
      Log.info("getLogicalNodes called at server on " + this.server.getPort());
      GenericArray<Utf8> out = new GenericData.Array<Utf8>(4,
          Schema.createArray(Schema.create(Type.STRING)));
      out.add(new Utf8("one"));
      out.add(new Utf8("two"));
      out.add(new Utf8("three"));
      out.add(new Utf8("four"));
      return out;
    }

    @Override
    public boolean heartbeat(Utf8 logicalNode, Utf8 physicalNode,
        Utf8 clienthost, FlumeNodeState s, long timestamp)
      throws AvroRemoteException {
      Log.info("heartbeat called at server on " + this.server.getPort());
      return true;
    }

    @Override
    public Void putReports(Map<Utf8, FlumeReportAvro> reports)
        throws AvroRemoteException {
      Log.info("putReports called at server on " + this.server.getPort());
      assertEquals(1, reports.size());
      FlumeReportAvro report = reports.get(new Utf8("reportKey"));
      assertNotNull(report);
      
      Map<Utf8, Long> longMetrics = report.longMetrics;
      assertEquals(2, longMetrics.size());
      
      assertEquals(Long.MIN_VALUE, ((Long) longMetrics.get(new Utf8("long1"))).longValue());
      assertEquals(Long.MAX_VALUE, ((Long) longMetrics.get(new Utf8("long2"))).longValue());
      
      Map<Utf8, Double> doubleMetrics = report.doubleMetrics;
      assertEquals(2, doubleMetrics.size());
      
      assertEquals(Double.MIN_VALUE, ((Double) doubleMetrics.get(new Utf8("double1"))).doubleValue(), 0.0001);
      assertEquals(Double.MAX_VALUE, ((Double) doubleMetrics.get(new Utf8("double2"))).doubleValue(), 0.0001);
      
      Map<Utf8, Utf8> stringMetrics = report.stringMetrics;
      assertEquals(2, stringMetrics.size());
      assertEquals("string1", stringMetrics.get(new Utf8("string1")).toString());
      assertEquals("string2", stringMetrics.get(new Utf8("string2")).toString());
      return null;
    }
  }
  
  /**
   * Mock ThriftServer.
   */
  public class MockThriftServer extends ThriftServer implements
  FlumeClientServer.Iface, MasterClientRPC {

    @Override
    public void acknowledge(String ackid) throws TException {      
    }

    @Override
    public boolean checkAck(String ackid) throws TException {
      Log.info("checkAck called at server on " + this.port);
      return true;
    }

    @Override
    public ThriftFlumeConfigData getConfig(String sourceId) throws TException {
      Log.info("getConfig called at server on " + this.port);
      FlumeConfigData out = new FlumeConfigData();
      out.flowID = "flowID";
      out.sinkConfig = "sinkConfig";
      out.sinkVersion = 112233;
      out.sourceConfig = "sourceConfig";
      out.sourceVersion = 445566;
      out.timestamp = 78901;
      return MasterClientServerThrift.configToThrift(out);
    }

    @Override
    public List<String> getLogicalNodes(String physNode) throws TException {
      Log.info("getLogicalNodes called at server on " + this.port);
      List<String> out = new LinkedList<String>();
      out.add("one");
      out.add("two");
      out.add("three");
      out.add("four");
      return out;
    }

    @Override
    public boolean heartbeat(String logicalNode, String physicalNode,
        String host, com.cloudera.flume.conf.thrift.FlumeNodeState s,
        long timestamp) throws TException {
      Log.info("heartbeat called at server on " + this.port);
      return true;
    }

    @Override
    public void putReports(Map<String, FlumeReport> reports) throws TException {
      Log.info("putReports called at server on " + this.port);
      assertEquals(1, reports.size());
      FlumeReport report = reports.get("reportKey");
      assertNotNull(report);
      
      Map<String, Long> longMetrics = report.longMetrics;
      assertEquals(2, longMetrics.size());
      
      assertEquals(Long.MIN_VALUE, ((Long) longMetrics.get("long1")).longValue());
      assertEquals(Long.MAX_VALUE, ((Long) longMetrics.get("long2")).longValue());
      
      Map<String, Double> doubleMetrics = report.doubleMetrics;
      assertEquals(2, doubleMetrics.size());
      
      assertEquals(Double.MIN_VALUE, ((Double) doubleMetrics.get("double1")).doubleValue(), .0001);
      assertEquals(Double.MAX_VALUE, ((Double) doubleMetrics.get("double2")).doubleValue(), .0001);
      
      Map<String, String> stringMetrics = report.stringMetrics;
      assertEquals(2, stringMetrics.size());
      assertEquals("string1", stringMetrics.get("string1").toString());
      assertEquals("string2", stringMetrics.get("string2").toString());
    }

    public void setPort(int port) {
      this.port = port;
    }
    
    @Override
    public void serve() throws IOException {
      LOG
      .info(String
          .format(
              "Starting blocking thread pool server for control server on port %d...",
              port));
      try {
        this.start(new FlumeClientServer.Processor((Iface) this), port,
            "MasterClientServer");
      } catch (TTransportException e) {
        throw new IOException(e.getMessage());
      }
    }
  }
  
  /**
   * Connect to stub server and make sure types are converted correctly.
   * 
   * We test both Avro and Thrift version of the client/server interaction.
   * @throws IOException 
   */
  @Test
  public void testConnect() throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.MASTER_HEARTBEAT_SERVERS,
        "localhost:44444");
    conf.set(FlumeConfiguration.MASTER_HEARBEAT_RPC, "AVRO");
    MultiMasterRPC masterRPC = new MultiMasterRPC(conf, false);
    MockAvroServer server1 = new MockAvroServer();
    server1.serve(44444);
    runTest(masterRPC);
    server1.stop();
    
    conf.set(FlumeConfiguration.MASTER_HEARBEAT_RPC, "THRIFT");
    masterRPC = new MultiMasterRPC(conf, false);
    MockThriftServer server2 = new MockThriftServer();
    server2.setPort(44444);
    server2.serve();
    runTest(masterRPC);
  }
  
  public void runTest(MultiMasterRPC masterRPC) throws IOException {
    assertEquals(true, masterRPC.checkAck("FOO"));
    List<String> logicalNodes = masterRPC.getLogicalNodes("Unused");
    assertEquals(4, logicalNodes.size());
    assertEquals("one", logicalNodes.get(0));
    assertEquals("two", logicalNodes.get(1));
    assertEquals("three", logicalNodes.get(2));
    assertEquals("four", logicalNodes.get(3));
    
    FlumeConfigData data = masterRPC.getConfig(new LogicalNode(null, ""));
    assertEquals("flowID", data.flowID);
    assertEquals("sinkConfig", data.sinkConfig);
    assertEquals(112233, data.sinkVersion);
    assertEquals("sourceConfig", data.sourceConfig);
    assertEquals(445566, data.sourceVersion);
    assertEquals(78901, data.timestamp);

    Map<String, Long> longMetrics = new HashMap<String, Long>();
    Map<String, String> stringMetrics = new HashMap<String, String>();
    Map<String, Double> doubleMetrics = new HashMap<String, Double>();
    
    longMetrics.put("long1", (long) Long.MIN_VALUE);
    longMetrics.put("long2", (long) Long.MAX_VALUE);
    
    stringMetrics.put("string1", "string1");
    stringMetrics.put("string2", "string2");      
    
    doubleMetrics.put("double1", Double.MIN_VALUE);
    doubleMetrics.put("double2", Double.MAX_VALUE);
    
    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    reports.put("reportKey", new ReportEvent(
        longMetrics, stringMetrics, doubleMetrics));
    
    masterRPC.putReports(reports);
  }
  
  @Test
  public void testTypeConversions() {
    // THRIFT NODE STATE
    assertEquals(StatusManager.NodeState.HELLO, MasterClientServerThrift
        .stateFromThrift(com.cloudera.flume.conf.thrift.FlumeNodeState.HELLO));
    assertEquals(StatusManager.NodeState.ACTIVE, MasterClientServerThrift
        .stateFromThrift(com.cloudera.flume.conf.thrift.FlumeNodeState.ACTIVE));
    assertEquals(StatusManager.NodeState.ERROR, MasterClientServerThrift
        .stateFromThrift(com.cloudera.flume.conf.thrift.FlumeNodeState.ERROR));
    assertEquals(StatusManager.NodeState.IDLE, MasterClientServerThrift
        .stateFromThrift(com.cloudera.flume.conf.thrift.FlumeNodeState.IDLE));
    assertEquals(StatusManager.NodeState.CONFIGURING, MasterClientServerThrift
        .stateFromThrift(com.cloudera.flume.conf.thrift.FlumeNodeState.CONFIGURING));

    assertEquals(com.cloudera.flume.conf.thrift.FlumeNodeState.HELLO, MasterClientServerThrift
        .stateToThrift(StatusManager.NodeState.HELLO));
    assertEquals(com.cloudera.flume.conf.thrift.FlumeNodeState.ACTIVE, MasterClientServerThrift
        .stateToThrift(StatusManager.NodeState.ACTIVE));
    assertEquals(com.cloudera.flume.conf.thrift.FlumeNodeState.ERROR, MasterClientServerThrift
        .stateToThrift(StatusManager.NodeState.ERROR));
    assertEquals(com.cloudera.flume.conf.thrift.FlumeNodeState.IDLE, MasterClientServerThrift
        .stateToThrift(StatusManager.NodeState.IDLE));
    assertEquals(com.cloudera.flume.conf.thrift.FlumeNodeState.CONFIGURING, MasterClientServerThrift
        .stateToThrift(StatusManager.NodeState.CONFIGURING));

    // AVRO NODE STATE
    assertEquals(StatusManager.NodeState.HELLO, MasterClientServerAvro
        .stateFromAvro(FlumeNodeState.HELLO));
    assertEquals(StatusManager.NodeState.ACTIVE, MasterClientServerAvro
        .stateFromAvro(FlumeNodeState.ACTIVE));
    assertEquals(StatusManager.NodeState.ERROR, MasterClientServerAvro
        .stateFromAvro(FlumeNodeState.ERROR));
    assertEquals(StatusManager.NodeState.IDLE, MasterClientServerAvro
        .stateFromAvro(FlumeNodeState.IDLE));
    assertEquals(StatusManager.NodeState.CONFIGURING, MasterClientServerAvro
        .stateFromAvro(FlumeNodeState.CONFIGURING));

    assertEquals(FlumeNodeState.HELLO, MasterClientServerAvro
        .stateToAvro(StatusManager.NodeState.HELLO));
    assertEquals(FlumeNodeState.ACTIVE, MasterClientServerAvro
        .stateToAvro(StatusManager.NodeState.ACTIVE));
    assertEquals(FlumeNodeState.ERROR, MasterClientServerAvro
        .stateToAvro(StatusManager.NodeState.ERROR));
    assertEquals(FlumeNodeState.IDLE, MasterClientServerAvro
        .stateToAvro(StatusManager.NodeState.IDLE));
    assertEquals(FlumeNodeState.CONFIGURING, MasterClientServerAvro
        .stateToAvro(StatusManager.NodeState.CONFIGURING));
  }
}