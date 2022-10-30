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

import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestPrometheusMetricsServer extends BaseHTTPMetricsTest {

  @Test
  public void testMetics() throws Exception {
    runLoad();

    testWithPort(getFreePort());

    shutdown();
  }

  private void testWithPort(int port) throws Exception {
    MonitorService srv = new PrometheusHTTPMetricsServer();
    Context context = new Context();
    context.put(PrometheusHTTPMetricsServer.CONFIG_PORT, String.valueOf(port));
    srv.configure(context);
    srv.start();
    Thread.sleep(1000);
    URL url = new URL("http://0.0.0.0:" + port + "/metrics");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(conn.getInputStream()));
    String line;
    String result = "";
    while ((line = reader.readLine()) != null) {
      result += line + "\n";
    }
    reader.close();
    String[] targetOutputs = {"ChannelSize{component=\"pmemChannel\",} 1.0\n",
      "Flume_ChannelSize{component=\"memChannel\",} 1.0\n",
      "Flume_ChannelCapacity{component=\"pmemChannel\",} 0.0\n",
      "Flume_ChannelCapacity{component=\"memChannel\",} 100.0\n",
      "Flume_EventPutAttemptCount_total{component=\"pmemChannel\",} 2.0\n",
      "Flume_EventPutAttemptCount_total{component=\"memChannel\",} 2.0\n",
      "Flume_EventTakeAttemptCount_total{component=\"pmemChannel\",} 1.0\n",
      "Flume_EventTakeAttemptCount_total{component=\"memChannel\",} 1.0\n",
      "Flume_EventPutSuccessCount_total{component=\"pmemChannel\",} 2.0\n",
      "Flume_EventPutSuccessCount_total{component=\"memChannel\",} 2.0\n",
      "Flume_EventTakeSuccessCount_total{component=\"pmemChannel\",} 1.0\n",
      "Flume_EventTakeSuccessCount_total{component=\"memChannel\",} 1.0\n"};

    for (String target : targetOutputs) {
      Assert.assertTrue(result.contains(target));
    }

    srv.stop();
  }

}
