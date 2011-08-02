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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeSpecException;

/**
 * This tests the basic json interface to the master's tables provided by
 * jersey.
 */
public class TestMasterJersey extends SetupMasterTestEnv {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestMasterJersey.class);

  /**
   * Gra b a url's contents. Since most are json, this should be small.
   * 
   * @param urlString
   * @return
   * @throws IOException
   */
  public static String curl(String urlString) throws IOException {
    URL url = new URL(urlString);
    URLConnection urlConn = url.openConnection();
    urlConn.setDoInput(true);
    urlConn.setUseCaches(false);

    int len = urlConn.getContentLength();
    String type = urlConn.getContentType();
    LOG.info("pulled " + urlString + "[ type=" + type + " len=" + len + "]");
    InputStreamReader isr = new InputStreamReader(urlConn.getInputStream());
    BufferedReader br = new BufferedReader(isr);
    StringBuilder sb = new StringBuilder();
    String s;
    while ((s = br.readLine()) != null) {
      sb.append(s);
      sb.append('\n');
    }
    return sb.toString();
  }

  @Test
  public void testMaster() throws IOException, InterruptedException,
      FlumeSpecException {
    String content = curl("http://localhost:35871/master");
    LOG.info("content: " + content);

    content = curl("http://localhost:35871/master/status");
    LOG.info("content: " + content);
    assertEquals("{\"status\":null}\n", content);

    content = curl("http://localhost:35871/master/commands");
    LOG.info("content: " + content);
    assertEquals("{\"commands\":null}\n", content);

    content = curl("http://localhost:35871/master/configs");
    LOG.info("content: " + content);
    assertEquals("{\"configs\":null}\n", content);

    content = curl("http://localhost:35871/master/configs/foo");
    LOG.info("content: " + content);
    assertEquals("", content);

    // Set a config, and show that json has ne config
    flumeMaster.specman.setConfig("foo", "foo", "null", "null");

    // excludes timestamp related json encoded stuff
    content = curl("http://localhost:35871/master/configs");
    LOG.info("content: " + content);
    assertTrue(content
        .contains("{\"configs\":{\"entry\":{\"key\":\"foo\","
            + "\"value\":{\"flowID\":\"foo\",\"sinkConfig\":\"null\",\"sinkVersion\""));
    assertTrue(content.contains("\"sourceConfig\":\"null\",\"sourceVersion\""));

    // excludes timestamp related json encoded stuff
    content = curl("http://localhost:35871/master/configs/foo");
    LOG.info("content: " + content);
    assertTrue(content
        .contains("{\"flowID\":\"foo\",\"sinkConfig\":\"null\",\"sinkVersion\""));
    assertTrue(content.contains("\"sourceConfig\":\"null\",\"sourceVersion\""));
  }

  /**
   * Test json interface for getting ack informaiton.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws FlumeSpecException
   */
  @Test
  public void testMasterAckManager() throws IOException, InterruptedException,
      FlumeSpecException {
    String content = curl("http://localhost:35871/master/acks");
    LOG.info("content: " + content);
    assertEquals("null\n", content);

    // Set a config, and show that json has ne config
    String foo = "foobarama is better than wackadoodles";
    flumeMaster.ackman.acknowledge(foo);

    // excludes timestamp related json encoded stuff
    content = curl("http://localhost:35871/master/acks");
    LOG.info("content: " + content);
    assertEquals(content, "{\"acks\":\"" + foo + "\"}\n");

    // Set a config, and show that json has ne config
    String baz = "bazapolooza is cool too";
    flumeMaster.ackman.acknowledge(baz);
    content = curl("http://localhost:35871/master/acks");
    LOG.info("content: " + content);
    assertEquals(content, "{\"acks\":[\"" + foo + "\",\"" + baz + "\"]}\n");

    // consume the ack
    flumeMaster.ackman.check("foobarama is better than wackadoodles");
    content = curl("http://localhost:35871/master/acks");
    LOG.info("content: " + content);
    assertEquals(content, "{\"acks\":\"" + baz + "\"}\n");

  }
}
