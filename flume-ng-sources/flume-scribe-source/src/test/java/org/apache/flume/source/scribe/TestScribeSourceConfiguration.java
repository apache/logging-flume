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

import org.apache.flume.Context;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestScribeSourceConfiguration {
  private static final Logger logger =
      LoggerFactory.getLogger(TestScribeSourceThriftServerFactory.class);
  Context context;
  ScribeSourceConfiguration configuration;


  @Before
  public void setUp() {
    context = new Context();
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    configuration = new ScribeSourceConfiguration();
  }

  @After
  public void tearDown() {
    context = null;
    configuration = null;
  }

  @Test
  public void testConfiguration() {
    context.put("port", "1111");
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    context.put("selectorThreads", "2");
    context.put("thriftServer",
        ScribeSourceConfiguration.ThriftServerType.TTHREADPOOL_SERVER.getValue());
    configuration.configure(context);
    Assert.assertEquals(configuration.port, 1111);
    Assert.assertEquals(configuration.workerThreadNum, 8);
    Assert.assertEquals(configuration.maxReadBufferBytes, 10240);
    Assert.assertEquals(configuration.selectorThreadNum, 2);
    Assert.assertTrue(
        configuration.thriftServerType ==
            ScribeSourceConfiguration.ThriftServerType.TTHREADPOOL_SERVER);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfigurationWithBadThriftServerType() {
    context.put("port", "1111");
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    context.put("selectorThreads", "2");
    context.put("thriftServer", "aaa");
    configuration.configure(context);
  }

  @Test
  public void testCreateTHsHasServerArg() throws Exception{
    context.put("port", "1111");
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    context.put("selectorThreads", "2");
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.ThriftServerType.THSHA_SERVER.getValue());
    configuration.configure(context);
    THsHaServer.Args args = configuration.createTHsHasServerArg(
        null, TestUtils.findFreePort());
    Assert.assertEquals(args.maxReadBufferBytes, 10240);
    Assert.assertEquals(args.getMaxWorkerThreads(), 8);
  }

  @Test
  public void testCreateTThreadedSelectorServerArgs() throws Exception {
    context.put("port", "1111");
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    context.put("selectorThreads", "2");
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.ThriftServerType.TTHREADED_SELECTOR_SERVER.getValue());
    configuration.configure(context);
    TThreadedSelectorServer.Args args =
        configuration.createTThreadedSelectorServerArgs(null, TestUtils.findFreePort());
    Assert.assertEquals(args.maxReadBufferBytes, 10240);
    Assert.assertEquals(args.selectorThreads, 2);
    Assert.assertEquals(args.getWorkerThreads(), 8);
  }

  @Test
  public void testCreateTThreadPoolServerArgs() throws Exception {
    context.put("port", "1111");
    context.put("workerThreads", "8");
    context.put("maxReadBufferBytes", "10240");
    context.put("maxThriftFrameSizeBytes", "1204");
    context.put("selectorThreads", "2");
    context.put(
        "thriftServer",
        ScribeSourceConfiguration.ThriftServerType.TTHREADPOOL_SERVER.getValue());
    configuration.configure(context);
    TThreadPoolServer.Args args =
        configuration.createTThreadPoolServerArgs(null, TestUtils.findFreePort());
    Assert.assertEquals(args.minWorkerThreads, 5);
    Assert.assertEquals(args.maxWorkerThreads, 8);
  }
}
