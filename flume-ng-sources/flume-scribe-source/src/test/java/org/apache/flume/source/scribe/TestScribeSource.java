/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.source.scribe;

import junit.framework.Assert;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TestScribeSource {

  private static int port;
  private static Channel memoryChannel;
  private static ScribeSource scribeSource;

  private static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    port = findFreePort();
    Context context = new Context();
    context.put("port", String.valueOf(port));

    scribeSource = new ScribeSource();
    scribeSource.setName("Scribe Source");

    Configurables.configure(scribeSource, context);

    memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);

    List<Channel> channels = new ArrayList<Channel>(1);
    channels.add(memoryChannel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    memoryChannel.start();

    scribeSource.setChannelProcessor(new ChannelProcessor(rcs));
    scribeSource.start();
  }

  @Test
  public void testScribeMessage() throws Exception {
    TTransport transport = new TFramedTransport(new TSocket("localhost", port));

    TProtocol protocol = new TBinaryProtocol(transport);
    Scribe.Client client = new Scribe.Client(protocol);
    transport.open();
    LogEntry logEntry = new LogEntry("INFO", "Sending info msg to scribe source");
    List<LogEntry> logEntries = new ArrayList<LogEntry>(1);
    logEntries.add(logEntry);
    client.Log(logEntries);

    // try to get it from Channels
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event e = memoryChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("Sending info msg to scribe source", new String(e.getBody()));
    tx.commit();
    tx.close();
  }

  @Test
  public void testScribeMultipleMessages() throws Exception {
    TTransport transport = new TFramedTransport(new TSocket("localhost", port));

    TProtocol protocol = new TBinaryProtocol(transport);
    Scribe.Client client = new Scribe.Client(protocol);
    transport.open();

    List<LogEntry> logEntries = new ArrayList<LogEntry>(10);
    for (int i = 0; i < 10; i++) {
      LogEntry logEntry = new LogEntry("INFO", String.format("Sending info msg# %d to scribe source", i));
      logEntries.add(logEntry);
    }

    client.Log(logEntries);

    // try to get it from Channels
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();

    for (int i = 0; i < 10; i++) {
      Event e = memoryChannel.take();
      Assert.assertNotNull(e);
      Assert.assertEquals(String.format("Sending info msg# %d to scribe source", i), new String(e.getBody()));
    }
    tx.commit();
    tx.close();
  }

  @AfterClass
  public static void cleanup() {
    memoryChannel.stop();
    scribeSource.stop();
  }

}