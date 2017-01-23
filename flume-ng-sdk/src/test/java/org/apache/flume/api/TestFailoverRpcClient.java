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

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestFailoverRpcClient {
  /**
   * Test a bunch of servers closing the one we are writing to and bringing
   * another one back online.
   *
   * @throws FlumeException
   * @throws EventDeliveryException
   * @throws InterruptedException
   */

  @Test
  public void testFailover()
      throws FlumeException, EventDeliveryException,InterruptedException {
    FailoverRpcClient client = null;
    Server server1 = RpcTestUtils.startServer(new OKAvroHandler());
    Server server2 = RpcTestUtils.startServer(new OKAvroHandler());
    Server server3 = RpcTestUtils.startServer(new OKAvroHandler());
    Properties props = new Properties();
    int s1Port = server1.getPort();
    int s2Port = server2.getPort();
    int s3Port = server3.getPort();
    props.put("client.type", "default_failover");
    props.put("hosts", "host1 host2 host3");
    props.put("hosts.host1", "127.0.0.1:" + String.valueOf(s1Port));
    props.put("hosts.host2", "127.0.0.1:" + String.valueOf(s2Port));
    props.put("hosts.host3", "127.0.0.1:" + String.valueOf(s3Port));
    client = (FailoverRpcClient) RpcClientFactory.getInstance(props);
    List<Event> events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    Assert.assertEquals(client.getLastConnectedServerAddress(),
        new InetSocketAddress("127.0.0.1", server1.getPort()));
    server1.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    Assert.assertEquals(new InetSocketAddress("localhost", server2.getPort()),
        client.getLastConnectedServerAddress());
    server2.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    client.append(EventBuilder.withBody("Had a sandwich?",
        Charset.forName("UTF8")));
    Assert.assertEquals(new InetSocketAddress("localhost", server3.getPort()),
        client.getLastConnectedServerAddress());
    // Bring server 2 back.
    Server server4 = RpcTestUtils.startServer(new OKAvroHandler(), s2Port);
    server3.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    Assert.assertEquals(new InetSocketAddress("localhost", s2Port),
        client.getLastConnectedServerAddress());

    Server server5 = RpcTestUtils.startServer(new OKAvroHandler(), s1Port);
    // Make sure we are still talking to server 4

    client
    .append(EventBuilder.withBody("Had a mango?", Charset.forName("UTF8")));
    Assert.assertEquals(new InetSocketAddress("localhost", s2Port),
        client.getLastConnectedServerAddress());
    server4.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    Assert.assertEquals(new InetSocketAddress("localhost", s1Port),
        client.getLastConnectedServerAddress());
    server5.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    Server server6 = RpcTestUtils.startServer(new OKAvroHandler(), s1Port);
    client.append(EventBuilder.withBody("Had a whole watermelon?", Charset.forName("UTF8")));
    Assert.assertEquals(new InetSocketAddress("localhost", s1Port),
        client.getLastConnectedServerAddress());

    server6.close();
    Thread.sleep(1000L); // wait a second for the close to occur
    Server server7 = RpcTestUtils.startServer(new OKAvroHandler(), s3Port);
    events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    Assert.assertEquals(new InetSocketAddress("localhost", s3Port),
        client.getLastConnectedServerAddress());
    server7.close();
  }

  /**
   * Try writing to some servers and then kill them all.
   *
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test(
      expected = EventDeliveryException.class)
  public void testFailedServers() throws FlumeException, EventDeliveryException {
    FailoverRpcClient client = null;
    Server server1 = RpcTestUtils.startServer(new OKAvroHandler());
    Server server2 = RpcTestUtils.startServer(new OKAvroHandler());
    Server server3 = RpcTestUtils.startServer(new OKAvroHandler());
    Properties props = new Properties();
    props.put("client.type", "default_failover");

    props.put("hosts", "host1 host2 host3");
    props.put("hosts.host1", "localhost:" + String.valueOf(server1.getPort()));
    props.put("hosts.host2", "localhost:" + String.valueOf(server2.getPort()));
    props.put("hosts.host3", " localhost:" + String.valueOf(server3.getPort()));
    client = (FailoverRpcClient) RpcClientFactory.getInstance(props);
    List<Event> events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
    server1.close();
    server2.close();
    server3.close();
    events = new ArrayList<Event>();
    for (int i = 0; i < 50; i++) {
      events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
    }
    client.appendBatch(events);
  }

}
