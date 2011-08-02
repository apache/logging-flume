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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer;
import com.cloudera.flume.conf.thrift.FlumeMasterCommand;
import com.cloudera.flume.conf.thrift.FlumeNodeStatus;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Client;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Iface;
import com.cloudera.flume.util.ThriftServer;

public class TestThriftServer extends TestCase {
  public static Logger LOG = Logger.getLogger(TestThriftServer.class);

  class MyThriftServer extends ThriftServer implements Iface {

    public void serve() throws TTransportException {
      LOG.info("Starting dummy server");
      this.start(new FlumeMasterAdminServer.Processor(this), 56789,
          "MyThriftServer");
    }

    @Override
    public Map<String, FlumeNodeStatus> getNodeStatuses() throws TException {
      return new HashMap<String, FlumeNodeStatus>();
    }

    @Override
    public boolean isFailure(long cmdid) throws TException {
      return true;
    }

    @Override
    public boolean isSuccess(long cmdid) throws TException {
      return false;
    }

    @Override
    public long submit(FlumeMasterCommand command) throws TException {
      return 42;
    }

    @Override
    public Map<String, ThriftFlumeConfigData> getConfigs() throws TException {
      return new HashMap<String, ThriftFlumeConfigData>();
    }

    @Override
    public boolean hasCmdId(long cmdid) throws TException {
      return true;
    }
  }

  public void testMasterAdminServer() throws TException, InterruptedException {
    MyThriftServer server = new MyThriftServer();
    server.serve();

    // Try connection
    TTransport masterTransport = new TSocket("localhost", 56789);
    TProtocol protocol = new TBinaryProtocol(masterTransport);
    masterTransport.open();
    Client client = new Client(protocol);
    long submit = client.submit(new FlumeMasterCommand());
    assertEquals("Expected response was 42, got " + submit, submit, 42);

    boolean succ = client.isSuccess(42);
    assertEquals("Expected response was false, got " + succ, succ, false);

    boolean fail = client.isFailure(42);
    assertEquals("Expected response was true, got " + fail, fail, true);

    Map<String, ThriftFlumeConfigData> cfgs = client.getConfigs();
    assertEquals("Expected response was 0, got " + cfgs.size(), cfgs.size(), 0);

    server.stop();
  }

  public void testThriftServerOpenClose() throws TTransportException {
    MyThriftServer server = new MyThriftServer();
    for (int i = 0; i < 50; i++) {
      LOG.info("open close " + i);
      server.serve();
      server.stop();
    }
  }
}
