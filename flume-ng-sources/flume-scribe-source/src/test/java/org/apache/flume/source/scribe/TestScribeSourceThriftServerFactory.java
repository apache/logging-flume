/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.source.scribe;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestScribeSourceThriftServerFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(TestScribeSourceThriftServerFactory.class);
  Context context;
  ScribeSource.Receiver receiver;
  ScribeSourceConfiguration configuration;

  @Before
  public void setUp() {
    int port = 11111;
    try {
      ServerSocket socket = new ServerSocket(0);
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException ex) {
      logger.error("Got exception while finding port", ex);
    }
    context = new Context();
    context.put("port", Integer.toString(port));
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    ScribeSource source = new ScribeSource();
    configuration = new ScribeSourceConfiguration();
    receiver = source.new Receiver();
  }

  @After
  public void tearDown() {
    context = null;
    receiver = null;
    configuration = null;
  }

  /**
   * This function has purposes:
   * 1. Test server's function.
   * 2. Release server port as TNonblockingServerSocket will
   * automatically bind port and call server.stop() won't help if server
   * hadn't be started.
   * @param server
   */
  public void testServer(final TServer server) {
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run(){
        server.serve();
      }
    });
    serverThread.start();
    while (!server.isServing()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    server.stop();
  }

  @Test
  public void testThshaServer() {
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.ThriftServerType.THSHA_SERVER.getValue());
    configuration.configure(context);
    TServer server = ScribeSourceThriftServerFactory.create(configuration, receiver);
    Assert.assertTrue(server instanceof THsHaServer);
    testServer(server);
  }

  @Test
  public void testTThreadedSelectorServer() {
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.
            ThriftServerType.TTHREADED_SELECTOR_SERVER.getValue());
    configuration.configure(context);
    TServer server = ScribeSourceThriftServerFactory.create(configuration, receiver);
    Assert.assertTrue(server instanceof TThreadedSelectorServer);
    testServer(server);
  }

  @Test
  public void testTThreadPoolServer() {
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.
            ThriftServerType.TTHREADPOOL_SERVER.getValue());
    configuration.configure(context);
    TServer server = ScribeSourceThriftServerFactory.create(configuration, receiver);
    Assert.assertTrue(server instanceof TThreadPoolServer);
    testServer(server);
  }

  @Test(expected = FlumeException.class)
  public void testIllegalThriftServer() {
    configuration.configure(context);
    configuration.thriftServerType = null;
    ScribeSourceThriftServerFactory.create(configuration, receiver);
  }
}
