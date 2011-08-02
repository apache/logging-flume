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
package com.cloudera.flume.handlers.avro;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;

/**
 *This is a sink that sends events to a remote host/port using Avro.
 */
public class AvroEventSink extends EventSink.Base {

  static final Logger LOG = LoggerFactory.getLogger(AvroEventSink.class);

  final public static String A_SERVERHOST = "serverHost";
  final public static String A_SERVERPORT = "serverPort";
  final public static String A_SENTBYTES = "sentBytes";

  protected FlumeEventAvroServer avroClient;
  String host;
  int port;
  HttpTransceiver transport;

  // this boolean variable is not used anywhere
  boolean nonblocking;
  /*
   * The following variables keeps track of the number of bytes of the
   * Event.body shipped.
   */
  AtomicLong sentBytes = new AtomicLong();

  public AvroEventSink(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void append(Event e) throws IOException {
    // convert the flumeEvent to AvroEevent
    AvroFlumeEvent afe = AvroEventAdaptor.convert(e);
    // Make sure client side is initialized.
    this.ensureInitialized();
    try {
      avroClient.append(afe);
      sentBytes.addAndGet(e.getBody().length);
      super.append(e);
    } catch (AvroRemoteException e1) {
      throw new IOException("Append failed " + e1.getMessage(), e1);
    }
  }

  private void ensureInitialized() throws IOException {
    if (this.avroClient == null || this.transport == null) {
      throw new IOException("MasterRPC called while not connected to master");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open() throws IOException {

    URL url = new URL("http", host, port, "/");
    transport = new HttpTransceiver(url);
    try {
      this.avroClient = (FlumeEventAvroServer) SpecificRequestor.getClient(
          FlumeEventAvroServer.class, transport);
    } catch (Exception e) {
      throw new IOException("Failed to open Avro event sink at " + host + ":"
          + port + " : " + e.getMessage());
    }
    LOG.info("AvroEventSink open on port  " + port);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (transport != null) {
      transport.close();
      transport = null;
      LOG.info("AvroEventSink on port " + port + " closed");
    } else {
      LOG.warn("Trying to close AvroEventSink, which was closed already");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setStringMetric(A_SERVERHOST, host);
    rpt.setLongMetric(A_SERVERPORT, port);
    rpt.setLongMetric(A_SENTBYTES, sentBytes.get());
    return rpt;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length > 2) {
          throw new IllegalArgumentException(
              "usage: avroSink([hostname, [portno]]) ");
        }
        String host = FlumeConfiguration.get().getCollectorHost();
        int port = FlumeConfiguration.get().getCollectorPort();
        if (args.length >= 1) {
          host = args[0];
        }

        if (args.length >= 2) {
          port = Integer.parseInt(args[1]);
        }
        return new AvroEventSink(host, port);
      }
    };
  }
}
