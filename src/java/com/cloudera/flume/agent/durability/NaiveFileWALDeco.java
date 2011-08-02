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
package com.cloudera.flume.agent.durability;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.DriverListener;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This decorator does write ahead logging using the NaiveFileWAL mechanism. It
 * has a subordinate thread that drains the events that have been written to
 * disk. Latches are used to maintain open and close semantics.
 */
public class NaiveFileWALDeco<S extends EventSink> extends
    EventSinkDecorator<S> {
  static Logger LOG = Logger.getLogger(NaiveFileWALDeco.class);

  final WALManager walman;
  final RollTrigger trigger;
  final AckListener queuer;
  final EventSinkDecorator<S> drainSink;
  final long checkMs;

  RollSink input;
  EventSource drainSource;
  Driver conn;
  Context ctx;

  CountDownLatch started = null; // blocks open until subthread is started
  volatile IOException lastExn = null;

  public NaiveFileWALDeco(Context ctx, S s, final WALManager walman,
      RollTrigger t, AckListener al, long checkMs) {
    super(s);
    this.ctx = ctx;
    this.walman = walman;
    this.trigger = t;
    this.queuer = new AckListener.Empty();
    this.drainSink = new AckChecksumRegisterer<S>(s, al);
    this.checkMs = checkMs;
  }

  CountDownLatch completed = null; // block close until subthread is completes

  public static class AckChecksumRegisterer<S extends EventSink> extends
      EventSinkDecorator<S> {
    AckListener q;

    public AckChecksumRegisterer(S s, AckListener q) {
      super(s);
      this.q = q;
    }

    @Override
    public void append(Event e) throws IOException {

      super.append(e);

      // (TODO) this could actually verify check sum correctness, but it is
      // generated locally

      // check for and attempt to register interest in tag's checksum
      byte[] btyp = e.get(AckChecksumInjector.ATTR_ACK_TYPE);

      if (btyp == null) {
        // nothing more to do
        return;
      }

      byte[] btag = e.get(AckChecksumInjector.ATTR_ACK_TAG);

      if (Arrays.equals(btyp, AckChecksumInjector.CHECKSUM_STOP)) {
        String k = new String(btag);
        LOG.info("Registering interest in checksum group called '" + k + "'");
        // Checksum stop marker: register interest
        q.end(k);
        return;
      }

    }
  }

  /**
   * TODO(jon): double check that the synchronization is appropriate here
   */
  @Override
  public synchronized void append(Event e) throws IOException {
    Preconditions.checkNotNull(sink);
    Preconditions.checkState(isOpen.get(),
        "NaiveFileWALDeco not open for append");
    if (lastExn != null) {
      throw lastExn;
    }
    input.append(e);
  }

  @Override
  public synchronized void close() throws IOException {
    Preconditions.checkNotNull(sink);

    input.close(); // prevent new data from entering.
    walman.stopDrains();
    try {
      LOG.debug("Waiting for subthread to complete .. ");
      if (conn == null) {
        LOG.warn("Driver was null, shutting down");
      } else {
        // conn.stop();
        conn.join(Long.MAX_VALUE);
        LOG.debug(".. subthread completed");
      }

    } catch (InterruptedException e) {
      LOG.error("WAL drain thread interrupted", e);
    }

    drainSource.close();
    // This sets isOpen == false
    super.close();

    try {
      completed.await();
    } catch (InterruptedException e) {
      LOG.error("WAL drain thread flush interrupted", e);
    }

    // This is throws an exception thrown by the subthread.
    if (lastExn != null) {
      IOException tmp = lastExn;
      lastExn = null;
      LOG.warn("Throwing exception from subthread");
      throw tmp;
    }
    LOG.debug("Closed DiskFailoverDeco");
  }

  @Override
  synchronized public void open() throws IOException {
    Preconditions.checkNotNull(sink);
    input = walman.getAckingSink(ctx, trigger, queuer, checkMs);

    drainSource = walman.getEventSource();

    // TODO (jon) catch exceptions here and close them before rethrowing

    // When this is open the sink is open from the callers point of view and we
    // can return.

    drainSink.open();
    input.open();
    drainSource.open();
    started = new CountDownLatch(1);
    completed = new CountDownLatch(1);

    conn = new DirectDriver("naive file wal transmit", drainSource, drainSink);
    /**
     * Do not take the lock on NaiveFileWALDeco.this in the connector listener,
     * as it's possible to deadlock within e.g. close()
     */
    conn.registerListener(new DriverListener.Base() {
      @Override
      public void fireStarted(Driver c) {
        started.countDown();
      }

      @Override
      public void fireStopped(Driver c) {
        completed.countDown();
      }

      @Override
      public void fireError(Driver c, Exception ex) {
        LOG.error("unexpected error with NaiveFileWALDeco", ex);
        lastExn = (ex instanceof IOException) ? (IOException) ex
            : new IOException(ex);
        try {
          conn.getSource().close();
          conn.getSink().close();
        } catch (IOException e) {
          LOG.error("Error closing", e);
        }
        completed.countDown();
        LOG.info("Error'ed Connector closed " + conn);
      }
    });
    conn.start();
    try {
      started.await();
    } catch (InterruptedException e) {
      LOG.error(e, e);
      throw new IOException(e);
    }
    LOG.debug("Opened NaiveFileWALDeco");
    Preconditions.checkNotNull(sink);
    Preconditions.checkState(!isOpen.get());
    // sink.open();
    isOpen.set(true);
  }

  public void setSink(S sink) {
    this.sink = sink;
    this.drainSink.setSink(sink);
  }

  public boolean rotate() {
    return input.rotate();
  }

  public boolean isEmpty() {
    return walman.isEmpty();
  }

  /**
   * End to end ack version, with logical node context (to isolate wals.)
   * **/
  public static SinkDecoBuilder builderEndToEndDir() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length <= 3,
            "usage: ackedWriteAhead[(maxMillis,walnode,checkMs)]");
        FlumeConfiguration conf = FlumeConfiguration.get();
        long delayMillis = conf.getAgentLogMaxAge();

        if (argv.length >= 1) {
          delayMillis = Long.parseLong(argv[0]);
        }

        // TODO (jon) this will cause problems with multiple nodes in same JVM
        FlumeNode node = FlumeNode.getInstance();
        String walnode = context.getValue(LogicalNodeContext.C_LOGICAL);
        if (argv.length >= 2) {
          walnode = argv[1];
        }
        if (walnode == null) {
          LOG.warn("Context does not have a logical node name "
              + "-- this will likely be a problem if you have multiple WALs");
        }

        long checkMs = 250; // TODO replace with config var;
        if (argv.length >= 3) {
          checkMs = Long.parseLong(argv[2]);
        }

        // TODO (jon) this is going to be unsafe because it creates before open.
        // This needs to be pushed into the logic of the decorator
        WALManager walman = node.getAddWALManager(walnode);
        return new NaiveFileWALDeco<EventSink>(context, null, walman,
            new TimeTrigger(delayMillis), node.getAckChecker()
                .getAgentAckQueuer(), checkMs);
      }
    };
  }

  @Override
  public String getName() {
    return "NaiveFileWAL";
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    ReportEvent walRpt = walman.getReport();
    rpt.merge(walRpt);
    ReportEvent sinkReport = sink.getReport();
    rpt.hierarchicalMerge(getName(), sinkReport);

    return rpt;
  }
}
