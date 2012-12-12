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
package org.apache.flume.agent.embedded;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestEmbeddedAgent {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestEmbeddedAgent.class);
  private static final String HOSTNAME = "localhost";
  private static AtomicInteger serialNumber = new AtomicInteger(0);
  private EmbeddedAgent agent;
  private Map<String, String> properties;
  private EventCollector eventCollector;
  private NettyServer nettyServer;
  private Map<String, String> headers;
  private byte[] body;


  @Before
  public void setUp() throws Exception {
    headers = Maps.newHashMap();
    headers.put("key1", "value1");
    body = "body".getBytes(Charsets.UTF_8);

    int port = findFreePort();
    eventCollector = new EventCollector();
    Responder responder = new SpecificResponder(AvroSourceProtocol.class,
        eventCollector);
    nettyServer = new NettyServer(responder,
              new InetSocketAddress(HOSTNAME, port));
    nettyServer.start();

    // give the server a second to start
    Thread.sleep(1000L);

    properties = Maps.newHashMap();
    properties.put("channel.type", "memory");
    properties.put("channel.capacity", "200");
    properties.put("sinks", "sink1 sink2");
    properties.put("sink1.type", "avro");
    properties.put("sink2.type", "avro");
    properties.put("sink1.hostname", HOSTNAME);
    properties.put("sink1.port", String.valueOf(port));
    properties.put("sink2.hostname", HOSTNAME);
    properties.put("sink2.port", String.valueOf(port));
    properties.put("processor.type", "load_balance");

    agent = new EmbeddedAgent("test-" + serialNumber.incrementAndGet());
  }
  @After
  public void tearDown() throws Exception {
    if(agent != null) {
      try {
        agent.stop();
      } catch (Exception e) {
        LOGGER.debug("Error shutting down agent", e);
      }
    }
    if(nettyServer != null) {
      try {
        nettyServer.close();
      } catch (Exception e) {
        LOGGER.debug("Error shutting down server", e);
      }
    }
  }
  @Test(timeout = 30000L)
  public void testPut() throws Exception {
    agent.configure(properties);
    agent.start();
    agent.put(EventBuilder.withBody(body, headers));

    Event event;
    while((event = eventCollector.poll()) == null) {
      Thread.sleep(500L);
    }
    Assert.assertNotNull(event);
    Assert.assertArrayEquals(body, event.getBody());
    Assert.assertEquals(headers, event.getHeaders());
  }
  @Test(timeout = 30000L)
  public void testPutAll() throws Exception {
    List<Event> events = Lists.newArrayList();
    events.add(EventBuilder.withBody(body, headers));
    agent.configure(properties);
    agent.start();
    agent.putAll(events);

    Event event;
    while((event = eventCollector.poll()) == null) {
      Thread.sleep(500L);
    }
    Assert.assertNotNull(event);
    Assert.assertArrayEquals(body, event.getBody());
    Assert.assertEquals(headers, event.getHeaders());
  }



  static class EventCollector implements AvroSourceProtocol {
    private final Queue<AvroFlumeEvent> eventQueue =
        new LinkedBlockingQueue<AvroFlumeEvent>();

    public Event poll() {
      AvroFlumeEvent avroEvent = eventQueue.poll();
      if(avroEvent != null) {
        return EventBuilder.withBody(avroEvent.getBody().array(),
            toStringMap(avroEvent.getHeaders()));
      }
      return null;
    }
    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      eventQueue.add(event);
      return Status.OK;
    }
    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {
      Preconditions.checkState(eventQueue.addAll(events));
      return Status.OK;
    }
  }
  private static Map<String, String> toStringMap(
      Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
        new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  private static int findFreePort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
        }
      }
    }
  }
}
