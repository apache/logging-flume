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
package org.apache.flume.test.agent;

import junit.framework.Assert;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.test.util.StagedInstall;
import org.junit.Test;

public class TestRpcClientCommunicationFailure {

  public static final String CONFIG_FILE_PRCCLIENT_TEST =
      "rpc-client-test.properties";

  @Test
  public void testFailure() throws Exception {
    try {

      StagedInstall.getInstance().startAgent(
          "rpccagent", CONFIG_FILE_PRCCLIENT_TEST);
      StagedInstall.waitUntilPortOpens("localhost", 12121, 20000);
      RpcClient client = RpcClientFactory.getDefaultInstance(
          "localhost", 12121);
      String[] text = {"foo", "bar", "xyz", "abc"};
      for (String str : text) {
        client.append(EventBuilder.withBody(str.getBytes()));
      }

      // Stop the agent
      StagedInstall.getInstance().stopAgent();

      // Try sending the event which should fail
      try {
        client.append(EventBuilder.withBody("test".getBytes()));
        Assert.fail("EventDeliveryException expected but not raised");
      } catch (EventDeliveryException ex) {
        System.out.println("Attempting to close client");
        client.close();
      }
    } finally {
      if (StagedInstall.getInstance().isRunning()) {
        StagedInstall.getInstance().stopAgent();
      }
    }
  }

}
