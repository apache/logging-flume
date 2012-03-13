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

package org.apache.flume.source.avroLegacy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import com.cloudera.flume.handlers.avro.AvroFlumeOGEvent;
import com.cloudera.flume.handlers.avro.FlumeOGEventAvroServer;
import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.ChannelException;

/**
 * <p>
 * A {@link Source} implementation that receives Avro events from Avro sink of
 * Flume OG</p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>host</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */

public class AvroLegacySource extends AbstractSource implements
    EventDrivenSource, Configurable, FlumeOGEventAvroServer {

  static final Logger LOG = LoggerFactory.getLogger(AvroLegacySource.class);

  //  Flume OG event fields
  public static final String HOST = "host";
  public static final String TIMESTAMP = "timestamp";
  public static final String PRIORITY = "pri";
  public static final String NANOS = "nanos";
  public static final String OG_EVENT = "FlumeOG";

  private CounterGroup counterGroup;
  protected FlumeOGEventAvroServer avroClient;
  private String host;
  private int port;
  private SpecificResponder res;
  private HttpServer http;

  public AvroLegacySource() {
    counterGroup = new CounterGroup();
  }

  @Override
  public void start() {
    // setup http server to receive OG events
    res = new SpecificResponder(FlumeOGEventAvroServer.class, this);
    try {
      http = new HttpServer(res, host, port);
    } catch (IOException eI) {
      LOG.warn("Failed to start server", eI);
      return;
    }
    http.start();
    super.start();
  }

  @Override
  public void stop() {
    http.close();
    super.stop();
  }

  @Override
  public Void append( AvroFlumeOGEvent evt ) throws AvroRemoteException {
    counterGroup.incrementAndGet("rpc.received");
    Map<String, String> headers = new HashMap<String, String>();

    // extract Flume OG event headers
    headers.put(HOST, evt.getHost().toString());
    headers.put(TIMESTAMP, evt.getTimestamp().toString());
    headers.put(PRIORITY, evt.getPriority().toString());
    headers.put(NANOS, evt.getNanos().toString());
    for (Entry<CharSequence, ByteBuffer> entry : evt.getFields().entrySet()) {
      headers.put(entry.getKey().toString(), entry.getValue().toString());
    }
    headers.put(OG_EVENT, "yes");

    Event event = EventBuilder.withBody(evt.getBody().array(), headers);
    try {
      getChannelProcessor().processEvent(event);
      counterGroup.incrementAndGet("rpc.events");
    } catch (ChannelException ex) {
      return null;
    }

    counterGroup.incrementAndGet("rpc.successful");
    return null;
  }

  @Override
  public void configure(Context context) {
    port = Integer.parseInt(context.getString("port"));
    host = context.getString("host");
  }

}
