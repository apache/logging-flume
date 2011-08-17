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
package com.cloudera.flume.agent;

import static com.cloudera.flume.master.TestMasterJersey.curl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.util.NetUtils;

public class TestNodeJersey {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestNodeJersey.class);

  FlumeNode node;

  @Before
  public void setup() throws IOException {
    node = FlumeNode.setup(new String[] { "-1" });
  }

  @After
  public void tearDown() {
    node.stop();
  }

  @Ignore
  @Test
  public void testNode() throws IOException, InterruptedException,
      FlumeSpecException, JSONException {
    String content = curl("http://localhost:35862/node/reports");
    LOG.info("content: " + content);
    JSONObject o = new JSONObject(content);
    assertNotNull(o.get("sysInfo"));
    assertNotNull(o.get("jvmInfo"));

    JSONObject localNode = o.getJSONObject("logicalnodes");
    String lnLnk = localNode.getString(NetUtils.localhost());

    content = curl(lnLnk);
    LOG.info("content: " + content);
    JSONObject node = new JSONObject(content);
    assertEquals(1, node.getInt("reconfigures"));
    assertEquals(NetUtils.localhost(), node.getString("physicalnode"));
    assertEquals("null", node.get("sinkConfig"));
    assertEquals("null", node.get("sourceConfig"));
    assertEquals("IDLE", node.get("state"));
  }
}
