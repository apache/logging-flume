/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.api;

import junit.framework.Assert;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class TestThriftRpcClient {
  private static final String SEQ = "sequence";
  private final Properties props = new Properties();
  ThriftRpcClient client;
  ThriftTestingSource src;
  private final Random random = new Random();
  int port;

  @Before
  public void setUp() throws Exception {
    props.setProperty("hosts", "h1");
    port = random.nextInt(40000) + 1024;
    props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
      "thrift");
    props.setProperty("hosts.h1", "0.0.0.0:"+ String.valueOf(port));
    props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "10");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT,
      "2000");
  }

  @After
  public void tearDown() throws Exception {
    src.stop();
  }

  /**
   * Insert events 0..count-1
   *
   * @param client
   * @param count
   * @throws Exception
   */
  public static void insertEvents(RpcClient client,
                                  int count) throws Exception {
    for (int i = 0; i < count; i++) {
      Map<String, String> header = new HashMap<String, String>();
      header.put(SEQ, String.valueOf(i));
      client.append(EventBuilder.withBody(String.valueOf(i).getBytes(),
        header));
    }
  }

  /**
   * Append events as a batch with seq starting at start and ending at limit.
   *
   * @param client
   * @param start
   * @param limit
   * @throws Exception
   */

  public static void insertAsBatch(RpcClient client, int start,
                                   int limit) throws Exception {
    List<Event> events = new ArrayList<Event>();
    for (int i = start; i <= limit; i++) {
      Map<String, String> header = new HashMap<String, String>();
      header.put(SEQ, String.valueOf(i));
      events.add(EventBuilder.withBody(String.valueOf(i).getBytes(), header));
    }
    client.appendBatch(events);
  }

  @Test
  public void testOK() throws Exception {
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.OK.name(),
      port);
    client = (ThriftRpcClient) RpcClientFactory.getInstance(props);
    insertEvents(client, 10); //10 events
    insertAsBatch(client, 10, 25); //16 events
    insertAsBatch(client, 26, 37); //12 events
    int count = 0;
    Assert.assertEquals(38, src.flumeEvents.size());
    for (Event e : src.flumeEvents) {
      Assert.assertEquals(new String(e.getBody()), String.valueOf(count++));
    }
    Assert.assertEquals(10, src.individualCount);
    Assert.assertEquals(4, src.batchCount);
    Assert.assertEquals(2, src.incompleteBatches);
  }

  @Test
  public void testSlow() throws Exception {
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.SLOW.name(),
      port);
    client = (ThriftRpcClient) RpcClientFactory.getInstance(props);
    insertEvents(client, 2); //2 events
    insertAsBatch(client, 2, 25); //24 events (3 batches)
    insertAsBatch(client, 26, 37); //12 events (2 batches)
    int count = 0;
    Assert.assertEquals(38, src.flumeEvents.size());
    for (Event e : src.flumeEvents) {
      Assert.assertEquals(new String(e.getBody()), String.valueOf(count++));
    }
    Assert.assertEquals(2, src.individualCount);
    Assert.assertEquals(5, src.batchCount);
    Assert.assertEquals(2, src.incompleteBatches);
  }

  @Test(expected = EventDeliveryException.class)
  public void testFail() throws Exception {
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.FAIL.name(),
      port);
    client = (ThriftRpcClient) RpcClientFactory.getInstance(props);
    insertEvents(client, 2); //2 events
    Assert.fail("Expected EventDeliveryException to be thrown.");
  }

  @Test
  public void testError() throws Throwable {
    try {
      src = new ThriftTestingSource(ThriftTestingSource.HandlerType.ERROR
        .name(), port);
      client = (ThriftRpcClient) RpcClientFactory.getThriftInstance("0.0.0" +
        ".0", port);
      insertEvents(client, 2); //2 events
    } catch (EventDeliveryException ex) {
      Assert.assertEquals("Failed to send event. ",
        ex.getMessage());
    }
  }

  @Test (expected = TimeoutException.class)
  public void testTimeout() throws Throwable {
    try {
      src = new ThriftTestingSource(ThriftTestingSource.HandlerType.TIMEOUT
        .name(), port);
      client = (ThriftRpcClient) RpcClientFactory.getThriftInstance(props);
      insertEvents(client, 2); //2 events
    } catch (EventDeliveryException ex) {
      throw ex.getCause();
    }
  }

  @Test
  public void testMultipleThreads() throws Throwable {
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.OK.name(),
      port);
    client = (ThriftRpcClient) RpcClientFactory.getThriftInstance("0.0.0" +
      ".0", port, 10);
    int threadCount = 100;
    ExecutorService submissionSvc = Executors.newFixedThreadPool(threadCount);
    ArrayList<Future<?>> futures = new ArrayList<Future<?>>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      futures.add(submissionSvc.submit(new Runnable() {
        @Override
        public void run() {
          try {
            insertAsBatch(client, 0, 9);
          } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use
            // File | Settings | File Templates.
          }
        }
      }));
    }
    for(int i = 0; i < threadCount; i++) {
      futures.get(i).get();
    }

    ArrayList<String> events = new ArrayList<String>();
    for(Event e: src.flumeEvents) {
      events.add(new String(e.getBody()));
    }
    int count = 0;
    Collections.sort(events);
    for (int i = 0; i < events.size();) {
      for(int j = 0; j < threadCount; j++) {
        Assert.assertEquals(String.valueOf(count), events.get(i++));
      }
      count++;
    }
  }
}
