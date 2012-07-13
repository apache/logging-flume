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

import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.event.EventBuilder;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.handlers.thrift.*;

public class ThriftLegacySource  extends AbstractSource implements
    EventDrivenSource, Configurable  {

  static final Logger LOG = LoggerFactory.getLogger(ThriftLegacySource.class);

  //  Old Flume event fields
  static final String HOST = "host";
  static final String TIMESTAMP = "timestamp";
  static final String PRIORITY = "pri";
  static final String NANOS = "nanos";
  static final String OG_EVENT = "FlumeOG";

  private CounterGroup counterGroup;
  private String host;
  private int port;
  private TServer server;
  private TServerTransport serverTransport;
  private Thread thriftHandlerThread;

  // Charset#decode is threadsafe.
  private Charset UTF_8 = Charset.forName("UTF-8");

  @SuppressWarnings("deprecation")
  private class ThriftFlumeEventServerImpl
        implements ThriftFlumeEventServer.Iface {
    
    public void append(ThriftFlumeEvent evt ) {
      if (evt == null) {
        return;
      }

      Map<String, String> headers = new HashMap<String, String>();
      // extract Flume event headers
      headers.put(HOST, evt.getHost());
      headers.put(TIMESTAMP, Long.toString(evt.getTimestamp()));
      headers.put(PRIORITY, evt.getPriority().toString());
      headers.put(NANOS, Long.toString(evt.getNanos()));
      for (Entry<String, ByteBuffer> entry: evt.getFields().entrySet()) {
        headers.put(entry.getKey().toString(),
          UTF_8.decode(entry.getValue()).toString());
      }
      headers.put(OG_EVENT, "yes");

      Event event = EventBuilder.withBody(evt.getBody(), headers);
      counterGroup.incrementAndGet("rpc.events");
      try {
        getChannelProcessor().processEvent(event);
      } catch (ChannelException ex) {
        LOG.warn("Failed to process event", ex);
        return;
      }

      counterGroup.incrementAndGet("rpc.successful");
      return;
    }

    public void close() {

    }
  }

  public static class ThriftHandler implements Runnable {
    private TServer server;

    public ThriftHandler(TServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      server.serve();
    }
  }

  @Override
  public void configure(Context context) {
    port = Integer.parseInt(context.getString("port"));
    host = context.getString("host");
  }

  public ThriftLegacySource() {
    counterGroup = new CounterGroup();
  }

  @SuppressWarnings("deprecation")
  @Override
  public void start() {
    try {
      InetSocketAddress bindAddr = new InetSocketAddress(host, port);
      serverTransport = new TServerSocket(bindAddr);
      ThriftFlumeEventServer.Processor processor =
          new ThriftFlumeEventServer.Processor(new ThriftFlumeEventServerImpl());
      server = new TThreadPoolServer(new TThreadPoolServer.
          Args(serverTransport).processor(processor));
    } catch (TTransportException e) {
      throw new FlumeException("Failed starting source", e);
    }
    ThriftHandler thriftHandler = new ThriftHandler(server);
    thriftHandlerThread = new Thread(thriftHandler);
    thriftHandlerThread.start();
    super.start();
  }

  @Override
  public void stop() {
    server.stop();
    serverTransport.close();
    try {
      thriftHandlerThread.join();
    } catch (InterruptedException eI) {
      LOG.warn("stop interrupted", eI);
      return;
    }
    super.stop();
  }

}
