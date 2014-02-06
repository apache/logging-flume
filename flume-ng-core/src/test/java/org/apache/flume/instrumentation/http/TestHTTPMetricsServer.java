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
package org.apache.flume.instrumentation.http;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXTestUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;

/**
 *
 */
public class TestHTTPMetricsServer {

  Channel memChannel = new MemoryChannel();
  Channel pmemChannel = new PseudoTxnMemoryChannel();
  Type mapType =
          new TypeToken<Map<String, Map<String, String>>>() {
          }.getType();
  Gson gson = new Gson();

  @Test
  public void testJSON() throws Exception {
    memChannel.setName("memChannel");
    pmemChannel.setName("pmemChannel");
    Context c = new Context();
    Configurables.configure(memChannel, c);
    Configurables.configure(pmemChannel, c);
    memChannel.start();
    pmemChannel.start();
    Transaction txn = memChannel.getTransaction();
    txn.begin();
    memChannel.put(EventBuilder.withBody("blah".getBytes()));
    memChannel.put(EventBuilder.withBody("blah".getBytes()));
    txn.commit();
    txn.close();

    txn = memChannel.getTransaction();
    txn.begin();
    memChannel.take();
    txn.commit();
    txn.close();


    Transaction txn2 = pmemChannel.getTransaction();
    txn2.begin();
    pmemChannel.put(EventBuilder.withBody("blah".getBytes()));
    pmemChannel.put(EventBuilder.withBody("blah".getBytes()));
    txn2.commit();
    txn2.close();

    txn2 = pmemChannel.getTransaction();
    txn2.begin();
    pmemChannel.take();
    txn2.commit();
    txn2.close();

    testWithPort(5467);
    testWithPort(33434);
    testWithPort(44343);
    testWithPort(0);
    memChannel.stop();
    pmemChannel.stop();
  }

  private void testWithPort(int port) throws Exception {
    MonitorService srv = new HTTPMetricsServer();
    Context context = new Context();
    if(port > 1024){
      context.put(HTTPMetricsServer.CONFIG_PORT, String.valueOf(port));
    } else {
      port = HTTPMetricsServer.DEFAULT_PORT;
    }
    srv.configure(context);
    srv.start();
    Thread.sleep(1000);
    URL url = new URL("http://0.0.0.0:" + String.valueOf(port) + "/metrics");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(conn.getInputStream()));
    String line;
    String result = "";
    while ((line = reader.readLine()) != null) {
      result += line;
    }
    reader.close();
    Map<String, Map<String, String>> mbeans = gson.fromJson(result, mapType);
    Assert.assertNotNull(mbeans);
    Map<String, String> memBean = mbeans.get("CHANNEL.memChannel");
    Assert.assertNotNull(memBean);
    JMXTestUtils.checkChannelCounterParams(memBean);
    Map<String, String> pmemBean = mbeans.get("CHANNEL.pmemChannel");
    Assert.assertNotNull(pmemBean);
    JMXTestUtils.checkChannelCounterParams(pmemBean);
    srv.stop();
    System.out.println(String.valueOf(port) + "test success!");
  }

  @Test
  public void testTrace() throws Exception {
    doTestForbiddenMethods(4543,"TRACE");
  }
  @Test
  public void testOptions() throws Exception {
    doTestForbiddenMethods(4432,"OPTIONS");
  }

  public void doTestForbiddenMethods(int port, String method)
    throws Exception {
    MonitorService srv = new HTTPMetricsServer();
    Context context = new Context();
    if (port > 1024) {
      context.put(HTTPMetricsServer.CONFIG_PORT, String.valueOf(port));
    } else {
      port = HTTPMetricsServer.DEFAULT_PORT;
    }
    srv.configure(context);
    srv.start();
    Thread.sleep(1000);
    URL url = new URL("http://0.0.0.0:" + String.valueOf(port) + "/metrics");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN,
      conn.getResponseCode());
    srv.stop();
  }
}
