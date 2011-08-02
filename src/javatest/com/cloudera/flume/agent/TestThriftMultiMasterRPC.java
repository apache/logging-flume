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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.ThriftFlumeClientServer;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeNodeState;
import com.cloudera.flume.conf.thrift.ThriftFlumeClientServer.Iface;
import com.cloudera.flume.master.MasterClientServerThrift;
import com.cloudera.flume.reporter.server.thrift.ThriftFlumeReport;
import com.cloudera.flume.util.ThriftServer;

/**
 * Tests for master failover from clients.
 */
public class TestThriftMultiMasterRPC {
  Logger LOG = Logger.getLogger(TestThriftMultiMasterRPC.class);

  /**
   * Mock ThriftServer.
   */
  class MyThriftServer extends ThriftServer implements Iface {
    boolean first = true;

    public void serve() throws TTransportException {
      serve(56789);
    }

    public void serve(int port) throws TTransportException {
      LOG.info("Starting dummy server");
      this.start(new ThriftFlumeClientServer.Processor(this), port, "MyThriftServer"
          + port);
    }

    @Override
    public void acknowledge(String ackid) throws TException {

    }

    @Override
    public boolean checkAck(String ackid) throws TException {
      if (first) {
        first = false;
        return true;
      }
      throw new TException("Throwing an exception");
    }

    @Override
    public ThriftFlumeConfigData getConfig(String sourceId) throws TException {
      return MasterClientServerThrift.configToThrift(new FlumeConfigData());
    }

    @Override
    public List<String> getLogicalNodes(String physNode) throws TException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean heartbeat(String logicalNode, String physicalNode,
        String clienthost, FlumeNodeState s, long timestamp) throws TException {
      return true;
    }

    @Override
    public void putReports(Map<String, ThriftFlumeReport> reports) throws TException {

    }

    @Override
    public Map<String, Integer> getChokeMap(String physNode) throws TException {
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
    conf.set(FlumeConfiguration.MASTER_HEARBEAT_RPC, "THRIFT");
    MultiMasterRPC masterRPC = new MultiMasterRPC(conf, false);
    MyThriftServer server1 = new MyThriftServer();
    server1.serve(56789);
    MyThriftServer server2 = new MyThriftServer();
    server2.serve(56790);

    masterRPC.checkAck("UNKNOWNACK");

    assertEquals("Port should have been 56789, got " + masterRPC.getCurPort(),
        56789, masterRPC.getCurPort());
    server1.stop();

    // This invocation will throw even if successfully called
    // (note that 'stop' is an optional method in TServer so may not do
    // anything)
    masterRPC.checkAck("UNKNOWNACK");
    assertEquals("Port should have been 56790, got " + masterRPC.getCurPort(),
        56790, masterRPC.getCurPort());
    masterRPC.close();

    server2.stop();
  }
}
