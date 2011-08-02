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

package com.cloudera.flume.handlers.thrift;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TSaneThreadPoolServer;
import org.apache.thrift.transport.TSaneServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.google.common.base.Preconditions;

/**
 * This sets up the port that listens for incoming flume event rpc calls. In
 * this version events are prioritized based on event priority and then by age
 * (older has higher priority). This doesn't have mechanism for dropping events
 * at the moment.
 * 
 * There is a problem with the nonblocking server -- for some reason (it gets
 * overwhelmed?, queue is full?), it will break by throwing an exception and
 * dropping the connection. This probably has something to do with functions
 * specified as "oneway" in thrift.
 * 
 */
public class PrioritizedThriftEventSource extends EventSource.Base {
  static int DEFAULT_QUEUE_SIZE = 1000;

  static final Logger LOG = LoggerFactory
      .getLogger(PrioritizedThriftEventSource.class);
  int port;
  ThriftFlumeEventServer svr;
  TSaneThreadPoolServer server;

  public static class EventQueue extends PriorityBlockingQueue<Event> {

    /**
     * (jon) don't know where this number comes from.
     */
    private static final long serialVersionUID = 7280524922090162382L;

    // We want to prioritize more important messages and older messages.
    public EventQueue(int size) {
      super(size, new Comparator<Event>() {
        @Override
        public int compare(Event o1, Event o2) {
          // higher priority entries are more important, and then older entries
          // are more important.
          int priorityDiff = o1.getPriority().ordinal()
              - o2.getPriority().ordinal();
          if (priorityDiff != 0)
            return priorityDiff;

          // smaller time means earlier and more important. Priority queue puts
          // takes the smallest first, which means we want a negative number if
          // o1 is older than o2.

          // Interesting -- we can get the same timestamp!
          long tdelta = (o1.getTimestamp() - o2.getTimestamp());
          if (tdelta != 0)
            return (int) tdelta;

          long ndelta = (o1.getNanos() - o2.getNanos());
          return (int) ndelta;
        }
      });
    }
  }

  final BlockingQueue<Event> q;

  /**
   * Creates a new prioritized event source on port port with event queue size
   * qsize.
   */
  public PrioritizedThriftEventSource(int port, int qsize) {
    this.port = port;
    this.svr = new ThriftFlumeEventServer();
    this.q = new EventQueue(qsize);
  }

  public PrioritizedThriftEventSource(int port) {
    this(port, DEFAULT_QUEUE_SIZE);
  }

  @Override
  public void open() throws IOException {

    try {
      ThriftFlumeEventServer.Processor processor = new ThriftFlumeEventServer.Processor(
          new ThriftFlumeEventServerImpl(new EventSink.Base() {
            @Override
            public void append(Event e) throws IOException,
                InterruptedException {
              q.add(e);
              super.append(e);
            }
          }));
      Factory protFactory = new TBinaryProtocol.Factory(true, true);

      TSaneServerSocket serverTransport = new TSaneServerSocket(port);
      server = new TSaneThreadPoolServer(processor, serverTransport,
          protFactory);
      LOG.info(String.format(
          "Starting blocking thread pool server on port %d...", port));

      server.start();

    } catch (TTransportException e) {
      e.printStackTrace();
      throw new IOException("Failed to create event server " + e);
    }
  }

  @Override
  public void close() throws IOException {
    if (server == null) {
      LOG.info(String.format("Server on port %d was already closed!", port));
      return;
    }
    server.stop();
    LOG.info(String.format("Closed server on port %d...", port));
  }

  @Override
  public Event next() throws IOException {

    try {
      Event e = q.take();
      updateEventProcessingStats(e);
      return e;
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new IOException("Waiting for queue element was interrupted! " + e);
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length == 1, "usage: tsource(port)");

        int port = Integer.parseInt(argv[0]);

        return new PrioritizedThriftEventSource(port);
      }

    };
  }
}
