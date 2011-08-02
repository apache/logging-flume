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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.specific.SpecificResponder;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.mortbay.log.Log;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeReportAvro;
import com.cloudera.flume.conf.avro.FlumeReportAvroServer;
import com.cloudera.flume.conf.avro.FlumeNodeState;
import com.cloudera.flume.master.MasterClientServerAvro;

public class TestAvroMultiMasterRPC {
  Logger LOG = Logger.getLogger(TestThriftMultiMasterRPC.class);

  /**
   * Mock AvroServer.
   */
  public class MockAvroServer implements FlumeReportAvroServer {
    boolean first = true;
    protected Server server;

    public void stop() {
      this.server.close();
    }

    public MockAvroServer() {

    }

    public void serve(int port) throws IOException {
      LOG
          .info(String
              .format(
                  "Starting blocking thread pool server for control server on port %d...",
                  port));
      SpecificResponder res = new SpecificResponder(
          FlumeReportAvroServer.class, this);
      this.server = new HttpServer(res, port);
      this.server.start();
    }

    @Override
    public java.lang.Void acknowledge(CharSequence ackid)
        throws AvroRemoteException {
      return null;
    }

    @Override
    public boolean checkAck(CharSequence ackid) throws AvroRemoteException {
      Log.info("Check-ack called at server on " + this.server.getPort());
      if (first) {
        first = false;
        return true;
      }
      Log.info("throwing an exception on " + this.server.getPort());
      throw new RuntimeException("Throwing an exception");
    }

    @Override
    public AvroFlumeConfigData getConfig(CharSequence sourceId)
        throws AvroRemoteException {
      return MasterClientServerAvro.configToAvro(new FlumeConfigData());
    }

    @Override
    public List<CharSequence> getLogicalNodes(CharSequence physNode)
        throws AvroRemoteException {
      return null;
    }

    @Override
    public boolean heartbeat(CharSequence logicalNode,
        CharSequence physicalNode, CharSequence clienthost, FlumeNodeState s,
        long timestamp) throws AvroRemoteException {
      return true;
    }

    @Override
    public Void putReports(Map<CharSequence, FlumeReportAvro> reports)
        throws AvroRemoteException {
      return null;
    }

    @Override
    public Map<CharSequence, Integer> getChokeMap(CharSequence physNode)
        throws AvroRemoteException {
      return null;
    }

  }

  /**
   * Tries to connect to several servers in turn and compensate as masters fail.
   */
  @Test
  public void testConnect() throws TTransportException, IOException,
      InterruptedException {
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.MASTER_HEARTBEAT_SERVERS,
        "localhost:9999,localhost:56789,localhost:56790");
    conf.set(FlumeConfiguration.MASTER_HEARBEAT_RPC, "AVRO");
    MultiMasterRPC masterRPC = new MultiMasterRPC(conf, false);
    MockAvroServer server1 = new MockAvroServer();
    server1.serve(56789);
    MockAvroServer server2 = new MockAvroServer();
    server2.serve(56790);

    assertEquals(true, masterRPC.checkAck("UNKNOWNACK")); // Server should roll
    // over to 56789
    assertEquals("Port should have been 56789, got " + masterRPC.getCurPort(),
        56789, masterRPC.getCurPort());

    masterRPC.checkAck("UNKNOWNACK"); // should cause exception, fail
    // over to 56790
    assertEquals("Port should have been 56790, got " + masterRPC.getCurPort(),
        56790, masterRPC.getCurPort());
    masterRPC.close();

    server2.stop();
  }
}
