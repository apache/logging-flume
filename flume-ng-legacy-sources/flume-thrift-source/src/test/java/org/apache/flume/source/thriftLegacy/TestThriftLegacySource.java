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

package org.apache.flume.source.thriftLegacy;

import com.cloudera.flume.handlers.thrift.Priority;
import com.cloudera.flume.handlers.thrift.ThriftFlumeEvent;
import com.cloudera.flume.handlers.thrift.ThriftFlumeEventServer.Client;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//EventStatus.java  Priority.java  ThriftFlumeEvent.java  ThriftFlumeEventServer.java

public class TestThriftLegacySource {

  private static final Logger logger = LoggerFactory
      .getLogger(ThriftLegacySource.class);

  private int selectedPort;
  private ThriftLegacySource source;
  private Channel channel;

  public class FlumeClient {
    private String host;
    private int port;

    public FlumeClient(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public void append(ThriftFlumeEvent evt) {
      TTransport transport;
      try {
        transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        Client client = new Client(protocol);
        transport.open();
        client.append(evt);
        transport.close();
      } catch (TTransportException e) {
        e.printStackTrace();
      } catch (TException e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() {
    source = new ThriftLegacySource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  private void bind() throws InterruptedException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("host", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (FlumeException e) {
        // Assume port in use, try another one
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
            source.getLifecycleState());
  }

  private void stop() throws InterruptedException {
    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    bind();
    stop();
  }

  @Test
  public void testRequest() throws InterruptedException, IOException {
    bind();

    Map flumeMap = new HashMap<CharSequence, ByteBuffer>();
    ThriftFlumeEvent thriftEvent =  new ThriftFlumeEvent(
        1, Priority.INFO, ByteBuffer.wrap("foo".getBytes()),
        0, "fooHost", flumeMap);
    FlumeClient fClient = new FlumeClient("0.0.0.0", selectedPort);
    fClient.append(thriftEvent);

    // check if the even has arrived in the channel through OG thrift source
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "foo",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();

    stop();
  }

  @Test
  public void testHeaders() throws InterruptedException, IOException {
    bind();

    Map flumeHeaders = new HashMap<CharSequence, ByteBuffer>();
    flumeHeaders.put("hello", ByteBuffer.wrap("world".getBytes("UTF-8")));
    ThriftFlumeEvent thriftEvent =  new ThriftFlumeEvent(
        1, Priority.INFO, ByteBuffer.wrap("foo".getBytes()),
        0, "fooHost", flumeHeaders);
    FlumeClient fClient = new FlumeClient("0.0.0.0", selectedPort);
    fClient.append(thriftEvent);

    // check if the event has arrived in the channel through OG thrift source
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Event in channel has our header", "world",
        event.getHeaders().get("hello"));
    transaction.commit();
    transaction.close();

    stop();
  }

}
