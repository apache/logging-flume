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

package com.cloudera.util;

import static com.cloudera.flume.master.TestMasterJersey.curl;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This tests the behavior of rapidly opening and closing the http server.
 * Things are sane.
 */
public class TestStatusHttpServer {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestStatusHttpServer.class);

  @Test
  public void testOpenClose() throws Exception {
    // Set directory of webapps to build-specific dir
    FlumeConfiguration.get().set(FlumeConfiguration.WEBAPPS_PATH,
        "build/webapps");

    FlumeConfiguration conf = FlumeConfiguration.get();
    String webPath = FlumeNode.getWebPath(conf);
    int port = FlumeConfiguration.get().getNodeStatusPort();
    StatusHttpServer http = new StatusHttpServer("flumeagent", webPath,
        "0.0.0.0", port, false);

    for (int i = 0; i < 50; i++) {
      http.start();
      http.stop();
    }
  }

  /**
   * This tests to make sure that auto find port works. Two http servers are
   * assigned to the same port -- the second one should detect the conflict and
   * then pick the next port to bind and serve from. curl will throw exception
   * on failure.
   */
  @Test
  @Ignore
  public void testAutoFindPort() throws IOException, Exception {
    // Set directory of webapps to build-specific dir
    FlumeConfiguration.get().set(FlumeConfiguration.WEBAPPS_PATH,
        "build/webapps");

    FlumeConfiguration conf = FlumeConfiguration.get();
    String webPath = FlumeNode.getWebPath(conf);
    int port = FlumeConfiguration.get().getNodeStatusPort();
    StatusHttpServer http = new StatusHttpServer("flumeagent", webPath,
        "0.0.0.0", port, true);
    http.start();

    StatusHttpServer http2 = new StatusHttpServer("flumeagent", webPath,
        "0.0.0.0", port, true);
    http2.start();

    String s1 = curl("http://localhost:35862");
    LOG.info("http1:" + s1);
    String s2 = curl("http://localhost:35863");
    LOG.info("http2:" + s2);
    http.stop();
    http2.stop();
  }
}
