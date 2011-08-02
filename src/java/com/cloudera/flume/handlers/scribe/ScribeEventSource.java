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

package com.cloudera.flume.handlers.scribe;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSaneServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.VersionInfo;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.util.ThriftServer;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * Acts as a scribe server
 */
public class ScribeEventSource extends ThriftServer implements EventSource,
    scribe.Iface {
  static final Logger LOG = Logger.getLogger(ScribeEventSource.class);
  final BlockingQueue<Event> pendingQueue = new LinkedBlockingQueue<Event>();

  final static public String SCRIBE_CATEGORY = "scribe.category";
  final AtomicBoolean running = new AtomicBoolean(false);
  long startedTime = 0;
  int port = 0;

  final static Event DONE_EVENT = new EventImpl(new byte[0]);

  /**
   * Construct a scribe event source.
   * 
   * @param port
   *          port the server will listen on
   */
  public ScribeEventSource(int port) {
    // turn off thrift strict read & write (respectively), otw legacy
    // thrift clients (ie scribe clients) won't be able to connect. This
    // mimics what scribed does.
    super(false, false);

    this.port = port;
  }

  public ScribeEventSource() {
    this(FlumeConfiguration.DEFAULT_SCRIBE_SOURCE_PORT);
  }

  /**
   * Stops the Thrift server
   */
  @Override
  public synchronized void close() throws IOException {
    running.set(false);
    this.stop();
    pendingQueue.add(DONE_EVENT);
  }

  /**
   * Blocks until a new event is available.
   */
  @Override
  public Event next() throws IOException {
    try {
      Event e = pendingQueue.take();
      if (e == DONE_EVENT) {
        return null;
      }
      return e;
    } catch (InterruptedException e) {
      LOG.error("ScribeEventSource was interrupted while waiting for an event",
          e);
      throw new IOException(e);
    }
  }

  /**
   * Starts a Thrift server and waits for it to come up.
   */
  @Override
  public synchronized void open() throws IOException {
    try {
      // Start the thrift server with a framed transport - suitable for
      // scribe clients
      this.start(new scribe.Processor(this), "ScribeEventSource",
          new TSaneServerSocket(port) {
            // we are providing the transport to ThriftServer -- the sole
            // job of this sane server subclass is to wrap the socket
            // with a framed transport
            protected TTransport acceptImpl() throws TTransportException {
              return new TFramedTransport(super.acceptImpl());
            }
          });
      running.set(true);
      startedTime = Clock.unixTime();
    } catch (TTransportException e) {
      LOG.error("Could not start Thrift Scribe server", e);
      throw new IOException(e);
    }
  }

  /**
   * This is the only API call required for scribe
   */
  @Override
  public ResultCode Log(List<LogEntry> messages) throws TException {
    // This will only happen if the log call arrives between
    // the Thrift socket opening and serve(...) returning - a small window!
    if (!running.get()) {
      return ResultCode.TRY_LATER;
    }
    for (LogEntry l : messages) {
      EventImpl e = new EventImpl(l.message.getBytes());
      e.set(SCRIBE_CATEGORY, l.category.getBytes());
      pendingQueue.add(e);
    }
    return ResultCode.OK;
  }

  /*
   * All following methods are required for fb303
   */

  @Override
  public long aliveSince() throws TException {
    return startedTime;
  }

  @Override
  public long getCounter(String key) throws TException {
    throw new TException("getCounter not implemented!");
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    throw new TException("getCounters not implemented!");
  }

  @Override
  public String getCpuProfile(int profileDurationInSec) throws TException {
    throw new TException("getCpuProfile not implemented!");
  }

  @Override
  public String getName() {
    return "Flume Scribe Event Server";
  }

  @Override
  public String getOption(String key) throws TException {
    throw new TException("getOption not implemented!");
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    throw new TException("getOptions not implemented!");
  }

  @Override
  public fb_status getStatus() throws TException {
    return running.get() ? fb_status.ALIVE : fb_status.STOPPED;
  }

  @Override
  public String getStatusDetails() throws TException {
    throw new TException("getStatusDetails not implemented!");
  }

  @Override
  public String getVersion() throws TException {
    return VersionInfo.getVersion();
  }

  @Override
  public void reinitialize() throws TException {
    throw new TException("reinitialize not implemented!");
  }

  @Override
  public void setOption(String key, String value) throws TException {
    throw new TException("setOption not implemented!");
  }

  @Override
  public void shutdown() throws TException {
    throw new TException("shutdown not implemented!");
  }

  /**
   * Builder takes one optional argument: the port to start on
   */
  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length <= 1, "usage: scribe[(port={"
            + FlumeConfiguration.DEFAULT_SCRIBE_SOURCE_PORT + "})]");
        int port = FlumeConfiguration.get().getScribeSourcePort();
        if (argv.length >= 1) {
          port = Integer.parseInt(argv[0]);
        }
        return new ScribeEventSource(port);
      }
    };
  }

  @Override
  public ReportEvent getReport() {
    // TODO(henry): add metrics
    // TODO missing EventSource stats
    return new ReportEvent("scribe-source");
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    reports.put(namePrefix + getName(), getReport());
  }
}
