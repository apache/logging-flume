/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.cloudera.util;

import static com.cloudera.util.HttpServerTestUtils.curlResp;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.BindException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.InternalHttpServer.ContextCreator;

public class InternalHttpServerTest {

  private static final Logger logger = LoggerFactory
      .getLogger(InternalHttpServerTest.class);

  private InternalHttpServer httpServer;

  @Before
  public void setUp() {
    httpServer = new InternalHttpServer();
  }

  @Test
  public void testStart() throws BindException {
    boolean success = false;

    httpServer.setContextCreator(new ContextCreator() {
      @Override
      public void addContexts(ContextHandlerCollection handlers) {
        InternalHttpServer.addHandlersFromPaths(handlers, new File(getClass()
            .getClassLoader().getResource("test-webroot").getFile()));
      }
    });

    try {
      httpServer.start();
      success = true;
    } catch (IllegalStateException e) {
      logger.error("Caught exception:", e);
    }

    Assert.assertTrue(success);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    success = false;

    try {
      httpServer.stop();
      success = true;
    } catch (IllegalStateException e) {
      logger.error("Caught exception:", e);
    }

    Assert.assertTrue(success);
  }

  /**
   * This tests to make sure that auto find port works. Two http servers are
   * assigned to the same port -- the second one should detect the conflict and
   * then pick the next port to bind and serve from. curl will throw exception
   * on failure.
   */
  @Test
  public void testAutoFindPort() throws IOException, Exception {
    int port = FlumeConfiguration.get().getNodeStatusPort();
    String bindAddress = "0.0.0.0";
    InternalHttpServer http = InternalHttpServer.startHttpServer(null,
        bindAddress, port);
    http.start();

    InternalHttpServer http2 = InternalHttpServer.startFindPortHttpServer(null,
        bindAddress, port);
    http2.start();

    // grab something from each server
    int port1 = http.getBoundPort();
    int resp1 = curlResp("http://localhost:" + port1);
    logger.info("http1 port:" + port1);
    
    int port2 = http2.getBoundPort();
    int resp2 = curlResp("http://localhost:" + port2);
    logger.info("http2 port:" + port2);

    // shutdown
    http.stop();
    http2.stop();

    assertEquals(404, resp1);
    assertEquals(404, resp2);
    assertEquals(port, port1);
    assertEquals(port + 1, port2);
  }
}
