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

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This tests the behavior of rapidly opening and closing the http server.
 * Things are sane.
 */
public class TestStatusHttpServer {

  @Test
  public void testOpenClose() throws IOException, Exception {
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
}
