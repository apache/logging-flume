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
import java.util.Map;

import junit.framework.TestCase;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeMasterAdminServerAvro;
import com.cloudera.flume.conf.avro.FlumeMasterCommandAvro;
import com.cloudera.flume.conf.avro.FlumeNodeStatusAvro;
import com.cloudera.flume.util.AdminRPC;
import com.cloudera.flume.util.AdminRPCAvro;

public class TestAvroAdminServer extends TestCase {
  public static Logger LOG = Logger.getLogger(TestAvroAdminServer.class);

  public class MyAvroServer implements FlumeMasterAdminServerAvro {
    private HttpServer server;
    
    public void serve() throws IOException {
      LOG.info("Starting dummy server");
      SpecificResponder res = new SpecificResponder(
          FlumeMasterAdminServerAvro.class, this);
      this.server = new HttpServer(res, 56789);
    }
    
    public void stop() {
      this.server.close();
    }
    
    @Override
    public Map<Utf8, FlumeNodeStatusAvro> getNodeStatuses()
        throws AvroRemoteException {
      return new HashMap<Utf8, FlumeNodeStatusAvro>();
    }
    
    @Override
    public Map<Utf8, GenericArray<Utf8>> getMappings(Utf8 physicalNode)
        throws AvroRemoteException {
      return new HashMap<Utf8, GenericArray<Utf8>>();
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
    public long submit(FlumeMasterCommandAvro command) throws AvroRemoteException {
      return 42;
    }

    @Override
    public Map<Utf8, AvroFlumeConfigData> getConfigs() throws AvroRemoteException {
      return new HashMap<Utf8, AvroFlumeConfigData>();
    }

    @Override
    public boolean hasCmdId(long cmdid) throws AvroRemoteException {
      return true;
    }
  }

  public void testMasterAdminServer() throws IOException {
    MyAvroServer server = new MyAvroServer();
    server.serve();

    AdminRPC client = new AdminRPCAvro("localhost", 56789);
    LOG.info("Connected to test master");
    
    
    long submit = client.submit(new Command(""));
    assertEquals("Expected response was 42, got " + submit, submit, 42);

    boolean succ = client.isSuccess(42);
    assertEquals("Expected response was false, got " + succ, succ, false);

    boolean fail = client.isFailure(42);
    assertEquals("Expected response was true, got " + fail, fail, true);

    Map<String, FlumeConfigData> cfgs = client.getConfigs();
    assertEquals("Expected response was 0, got " + cfgs.size(), cfgs.size(), 0);

    server.stop();
  }

  public void testAvroServerOpenClose() throws IOException {
    MyAvroServer server = new MyAvroServer();
    for (int i = 0; i < 50; i++) {
      LOG.info("open close " + i);
      server.serve();
      server.stop();
    }
  }
}
