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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.CommandStatusAvro;
import com.cloudera.flume.conf.avro.FlumeMasterAdminServerAvro;
import com.cloudera.flume.conf.avro.FlumeMasterCommandAvro;
import com.cloudera.flume.conf.avro.FlumeNodeStatusAvro;
import com.cloudera.flume.util.AdminRPC;
import com.cloudera.flume.util.AdminRPCAvro;

public class TestAvroAdminServer {

  public static final Logger LOG = LoggerFactory.getLogger(TestAvroAdminServer.class);

  public class MyAvroServer implements FlumeMasterAdminServerAvro {

    private HttpServer server;

    public void serve() throws IOException {
      LOG.info("Starting dummy server");
      SpecificResponder res = new SpecificResponder(
          FlumeMasterAdminServerAvro.class, this);
      this.server = new HttpServer(res, 56789);
      this.server.start();
    }

    public void stop() {
      this.server.close();
    }

    @Override
    public Map<CharSequence, FlumeNodeStatusAvro> getNodeStatuses()
        throws AvroRemoteException {
      return new HashMap<CharSequence, FlumeNodeStatusAvro>();
    }

    @Override
    public Map<CharSequence, List<CharSequence>> getMappings(
        CharSequence physicalNode) throws AvroRemoteException {
      return new HashMap<CharSequence, List<CharSequence>>();
    }

    @Override
    public boolean isFailure(long cmdid) throws AvroRemoteException {
      return true;
    }

    @Override
    public boolean isSuccess(long cmdid) throws AvroRemoteException {
      return false;
    }

    @Override
    public long submit(FlumeMasterCommandAvro command)
        throws AvroRemoteException {
      return 42;
    }

    @Override
    public Map<CharSequence, AvroFlumeConfigData> getConfigs()
        throws AvroRemoteException {
      return new HashMap<CharSequence, AvroFlumeConfigData>();
    }

    @Override
    public boolean hasCmdId(long cmdid) throws AvroRemoteException {
      return true;
    }

    @Override
    public CommandStatusAvro getCmdStatus(long cmdid)
        throws AvroRemoteException {
      if (cmdid == 1337) {
        List<CharSequence> l = new ArrayList<CharSequence>();
        l.add("arg1");
        l.add("arg2");
        CommandStatusAvro csa = new CommandStatusAvro();
        csa.cmdId = cmdid;
        csa.state = CommandStatus.State.SUCCEEDED.toString();
        csa.message = "message";
        csa.cmd = new FlumeMasterCommandAvro();
        csa.cmd.command = "cmd";
        csa.cmd.arguments = l;
        return csa;
      }
      return null;
    }
  }

  @Test
  public void testMasterAdminServer() throws IOException {
    MyAvroServer server = new MyAvroServer();
    server.serve();
    AdminRPC client = new AdminRPCAvro("localhost", 56789);
    LOG.info("Connected to test master");

    long submit = client.submit(new Command(""));
    Assert.assertEquals("Expected response was 42, got " + submit, submit, 42);

    boolean succ = client.isSuccess(42);
    Assert
        .assertEquals("Expected response was false, got " + succ, succ, false);

    boolean fail = client.isFailure(42);
    Assert.assertEquals("Expected response was true, got " + fail, fail, true);

    Map<String, FlumeConfigData> cfgs = client.getConfigs();
    Assert.assertEquals("Expected response was 0, got " + cfgs.size(), cfgs
        .size(), 0);

    CommandStatus cs = client.getCommandStatus(1337);
    Assert.assertEquals(1337, cs.getCmdID());
    Assert.assertEquals("message", cs.getMessage());
    Command cmd = cs.getCommand();
    Assert.assertEquals("cmd", cmd.getCommand());
    Assert.assertEquals("arg1", cmd.getArgs()[0]);
    Assert.assertEquals("arg2", cmd.getArgs()[1]);

    server.stop();
  }

  public void testMasterAdminServerBad() throws IOException {
    MyAvroServer server = new MyAvroServer();
    server.serve();
    AdminRPC client = new AdminRPCAvro("localhost", 56789);
    LOG.info("Connected to test master");

    try {
      CommandStatus bad = client.getCommandStatus(1234);
    } catch (AvroRuntimeException are) {
      // success!
      return;
    } finally {
      server.stop();
    }

    Assert.fail("should have thrown exception");
  }

  @Test
  public void testAvroServerOpenClose() throws IOException {
    MyAvroServer server = new MyAvroServer();
    for (int i = 0; i < 50; i++) {
      LOG.info("open close " + i);
      server.serve();
      server.stop();
    }
  }
}
