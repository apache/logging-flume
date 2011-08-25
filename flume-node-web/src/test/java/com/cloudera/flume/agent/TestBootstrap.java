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
package com.cloudera.flume.agent;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.HttpServerTestUtils;
import com.cloudera.util.InternalHttpServer;
import com.cloudera.util.InternalHttpServer.ContextCreator;

public class TestBootstrap {

  private static final Logger logger = LoggerFactory
      .getLogger(TestBootstrap.class);

  private InternalHttpServer httpServer;

  @Before
  public void setUp() {
    httpServer = new InternalHttpServer();
  }

  @Test
  public void testBootstrap() throws InterruptedException, IOException {
    Assert.assertNotNull(httpServer);

    logger.debug("httpServer:{}", httpServer);

    httpServer.setPort(0);
    httpServer.setContextCreator(new ContextCreator() {
      @Override
      public void addContexts(ContextHandlerCollection handlers) {
        InternalHttpServer.addHandlersFromPaths(handlers, new File("src/main"));
      }
    });

    httpServer.start();
    int port = httpServer.getBoundPort();
    String url = "http://localhost:" + port;
    logger.debug("Grabbing http response from " + url);
    int resp = HttpServerTestUtils.curlResp(url);
    httpServer.stop();
    assertEquals(resp, 200); // expect ok response code.
  }
}
