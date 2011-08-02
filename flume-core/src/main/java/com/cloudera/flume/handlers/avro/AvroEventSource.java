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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This sets up the port that listens for incoming flumeAvroEvent rpc calls
 * using Avro. This class pretty much mimics ThriftEventSource.
 */
public class AvroEventSource extends EventSource.Base {
  /*
   * In this version I am setting the following constants same as for the thrift
   * case. Seems like these constants don't really need to depend on the
   * underlying implementation, so maybe we can give them more general names
   * later.
   */
  final static int DEFAULT_QUEUE_SIZE = FlumeConfiguration.get()
      .getThriftQueueSize();
  final static long MAX_CLOSE_SLEEP = FlumeConfiguration.get()
      .getThriftCloseMaxSleep();

  static final Logger LOG = LoggerFactory.getLogger(AvroEventSource.class);

  public static final String A_SERVERPORT = "serverPort";
  public static final String A_QUEUE_CAPACITY = "queueCapacity";
  public static final String A_QUEUE_FREE = "queueFree";
  public static final String A_ENQUEUED = "enqueued";
  public static final String A_DEQUEUED = "dequeued";
  // BytesIN in here (unlike the Thrift version) corresponds to the total bytes
  // of Event.body shipped.
  public static final String A_BYTES_IN = "bytesIn";
  final int port;
  private FlumeEventAvroServerImpl svr;
  final BlockingQueue<Event> q;
  final AtomicLong enqueued = new AtomicLong();
  final AtomicLong dequeued = new AtomicLong();
  final AtomicLong bytesIn = new AtomicLong();

  boolean closed = true;

  /**
   * Create a Avro event source listening on port with a qsize buffer.
   */
  public AvroEventSource(int port, int qsize) {
    this.port = port;
    this.svr = new FlumeEventAvroServerImpl(port);
    this.q = new LinkedBlockingQueue<Event>(qsize);
  }

  /**
   * Get reportable data from the Avro event source.
   */
  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setLongMetric(A_SERVERPORT, port);
    rpt.setLongMetric(A_QUEUE_CAPACITY, q.size());
    rpt.setLongMetric(A_QUEUE_FREE, q.remainingCapacity());
    rpt.setLongMetric(A_ENQUEUED, enqueued.get());
    rpt.setLongMetric(A_DEQUEUED, dequeued.get());
    rpt.setLongMetric(A_BYTES_IN, bytesIn.get());
    return rpt;
  }

  /**
   * This constructor allows the for an arbitrary blocking queue implementation.
   */
  public AvroEventSource(int port, BlockingQueue<Event> q) {
    Preconditions.checkNotNull(q);
    this.port = port;
    this.q = q;
  }

  public AvroEventSource(int port) {
    this(port, DEFAULT_QUEUE_SIZE);
  }

  /**
   * Exposed for testing.
   */
  void enqueue(Event e) throws IOException {
    try {
      q.put(e);
      enqueued.getAndIncrement();
      bytesIn.getAndAdd(e.getBody().length);
    } catch (InterruptedException e1) {
      LOG.error("blocked append was interrupted", e1);
      throw new IOException(e1);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void open() throws IOException {

    this.svr = new FlumeEventAvroServerImpl(port) {
      @Override
      public void append(AvroFlumeEvent evt) {
        // convert AvroEvent evt -> e
        AvroEventAdaptor adapt = new AvroEventAdaptor(evt);
        try {
          enqueue(adapt.toFlumeEvent());
        } catch (IOException e1) {
          e1.printStackTrace();
        }
        super.append(evt);
      }
    };
    LOG.info(String.format("Avro listening server on port %d...", port));
    this.svr.start();
    this.closed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void close() throws IOException {

    long sz = q.size();
    LOG.info(String.format("Queue still has %d elements ...", sz));

    // Close down the server
    this.svr.close();

    // drain the queue
    // TODO (jon) parameterize queue drain max sleep is one minute
    long maxSleep = MAX_CLOSE_SLEEP;
    long start = Clock.unixTime();
    while (q.peek() != null) {
      if (Clock.unixTime() - start > maxSleep) {
        if (sz == q.size()) {
          // no progress made, timeout and close it.
          LOG
              .warn("Close timed out due to no progress.  Closing despite having "
                  + q.size() + " values still enqueued");
          return;
        }
        // there was some progress, go another cycle.
        start = Clock.unixTime();
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Unexpected interrupt of close " + e.getMessage(), e);
        Thread.currentThread().interrupt();
        closed = true;
        throw new IOException(e);
      }
    }

    closed = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event next() throws IOException {
    try {
      Event e = null;
      // block until an event shows up
      while ((e = q.poll(100, TimeUnit.MILLISECONDS)) == null) {

        synchronized (this) {
          // or bail out if closed
          if (closed) {
            return null;
          }
        }
      }
      // return the event
      synchronized (this) {
        dequeued.getAndIncrement();
        updateEventProcessingStats(e);
        return e;
      }
    } catch (InterruptedException e) {
      throw new IOException("Waiting for queue element was interrupted! "
          + e.getMessage(), e);
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        Preconditions
            .checkArgument(argv.length == 1, "usage: avroSource(port)");
        int port = Integer.parseInt(argv[0]);
        return new AvroEventSource(port);
      }
    };
  }
}
