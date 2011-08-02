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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.thrift.CommandStatusThrift;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Iface;
import com.cloudera.flume.conf.thrift.FlumeMasterCommandThrift;
import com.cloudera.flume.conf.thrift.FlumeNodeStatusThrift;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.util.AdminRPC;
import com.cloudera.flume.util.AdminRPCThrift;
import com.cloudera.flume.util.ThriftServer;

public class TestThriftAdminServer {

  public static Logger LOG = Logger.getLogger(TestThriftAdminServer.class);

  class MyThriftServer extends ThriftServer implements Iface {

    public void serve() throws TTransportException {
      LOG.info("Starting dummy server");
      this.start(new FlumeMasterAdminServer.Processor(this), 56789,
          "MyThriftServer");
    }

    @Override
    public Map<String, FlumeNodeStatusThrift> getNodeStatuses()
        throws TException {
      return new HashMap<String, FlumeNodeStatusThrift>();
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
    public long submit(FlumeMasterCommandThrift command) throws TException {
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

    @Override
    public Map<String, List<String>> getMappings(String physicalNode)
        throws TException {
      return new HashMap<String, List<String>>();
    }

    @Override
    public CommandStatusThrift getCmdStatus(long cmdid) throws TException {
      if (cmdid == 1337) {
        List<String> l = new ArrayList<String>();
        l.add("arg1");
        l.add("arg2");
        return new CommandStatusThrift(cmdid, CommandStatus.State.SUCCEEDED
            .toString(), "message", new FlumeMasterCommandThrift("cmd", l));
      }
      return null;
    }
  }

  @Test
  public void testMasterAdminServer() throws IOException, TTransportException {
    MyThriftServer server = new MyThriftServer();
    server.serve();

    AdminRPC client = new AdminRPCThrift("localhost", 56789);
    LOG.info("Connected to test master");

    long submit = client.submit(new Command(""));
    Assert.assertEquals("Expected response was 42, got " + submit, submit, 42);

    boolean succ = client.isSuccess(42);
    Assert.assertEquals("Expected response was false, got " + succ, succ, false);

    boolean fail = client.isFailure(42);
    Assert.assertEquals("Expected response was true, got " + fail, fail, true);

    Map<String, FlumeConfigData> cfgs = client.getConfigs();
    Assert.assertEquals("Expected response was 0, got " + cfgs.size(), cfgs.size(), 0);

    Map<String, List<String>> mappings = client.getMappings(null);
    Assert.assertEquals("Expected response was 0 got " + mappings.size(), mappings
        .size(), 0);

    CommandStatus cs = client.getCommandStatus(1337);
    assertEquals(1337, cs.getCmdID());
    assertEquals("message", cs.getMessage());
    Command cmd = cs.getCommand();
    assertEquals("cmd", cmd.getCommand());
    assertEquals("arg1", cmd.getArgs()[0]);
    assertEquals("arg2", cmd.getArgs()[1]);

    server.stop();
  }

  @Test
  public void testMasterAdminServerBad() throws TTransportException,
      IOException {
    MyThriftServer server = new MyThriftServer();
    server.serve();
    AdminRPC client = new AdminRPCThrift("localhost", 56789);
    LOG.info("Connected to test master");

    try {
      CommandStatus bad = client.getCommandStatus(1234);
    } catch (IOException ioe) {
      // success!
      return;
    } finally {
      server.stop();
    }

    fail("should have thrown exception");
  }

  @Test
  public void testThriftServerOpenClose() throws TTransportException {
    MyThriftServer server = new MyThriftServer();
    for (int i = 0; i < 50; i++) {
      LOG.info("open close " + i);
      server.serve();
      server.stop();
    }
  }
}
