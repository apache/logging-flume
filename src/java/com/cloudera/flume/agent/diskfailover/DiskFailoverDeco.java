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
package com.cloudera.flume.agent.diskfailover;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.ConnectorListener;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This decorator does failover handling using the NaiveFileFailover mechanism.
 * It has a subordinate thread that drains the events that have been written to
 * disk. Latches are used to maintain open and close semantics.
 */
public class DiskFailoverDeco<S extends EventSink> extends
    EventSinkDecorator<S> {
  static Logger LOG = Logger.getLogger(DiskFailoverDeco.class);

  final DiskFailoverManager dfoMan;
  final RollTrigger trigger;

  RollSink input;
  EventSource output;
  Driver conn;

  CountDownLatch completed = null; // block close until subthread is completed
  CountDownLatch started = null; // blocks open until subthread is started
  volatile IOException lastExn = null;

  final long checkmillis;

  public DiskFailoverDeco(S s, final DiskFailoverManager dfoman, RollTrigger t,
      long checkmillis) {
    super(s);
    this.dfoMan = dfoman;
    this.trigger = t;
    this.checkmillis = checkmillis;
  }

  /**
   * TODO(jon): double check that the synchronization is appropriate here
   */
  @Override
  public synchronized void append(Event e) throws IOException {
    Preconditions.checkNotNull(sink);
    Preconditions.checkArgument(isOpen.get(),
        "DiskFailoverDeco not open for append");

    if (lastExn != null) {
      throw lastExn;
    }

    input.append(e);
  }

  @Override
  public synchronized void close() throws IOException {
    Preconditions.checkNotNull(sink);
    LOG.debug("Closing DiskFailoverDeco");

    input.close(); // prevent new data from entering.
    dfoMan.close(); // put wal man into closing mode
    try {
      // wait for sub-thread to complete.
      LOG.debug("Waiting for subthread to complete .. ");
      conn.join();
      LOG.debug(".. subthread to completed");
    } catch (InterruptedException e) {
      LOG.error("WAL drain thread was interrupted", e);
    }

    output.close();
    super.close();

    try {
      completed.await();
    } catch (InterruptedException e) {
      LOG.error("DFO closing flush was interrupted", e);
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
    LOG.debug("Opening DiskFailoverDeco");
    input = dfoMan.getEventSink(trigger);
    output = dfoMan.getEventSource();

    // TODO (jon) catch exceptions here and close them before rethrowing
    super.open();
    input.open();
    output.open();
    started = new CountDownLatch(1);
    completed = new CountDownLatch(1);

    conn = new DirectDriver("FileFailover", output, sink);
    /**
     * Don't synchronize on DiskFailoverDeco.this in the ConnectorListener
     * otherwise you might get a deadlock.
     */
    conn.registerListener(new ConnectorListener.Base() {
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
        LOG.error("unexpected error with DiskFailoverDeco", ex);
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
    LOG.debug("Opened DiskFailoverDeco");
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length <= 1,
            "usage: diskFailover[(maxMillis[, checkmillis])]");
        FlumeConfiguration conf = FlumeConfiguration.get();
        long delayMillis = conf.getAgentLogMaxAge();

        if (argv.length >= 1) {
          delayMillis = Long.parseLong(argv[0]);
        }

        long checkmillis = 250;
        if (argv.length >= 2) {
          checkmillis = Long.parseLong(argv[1]);
        }

        // TODO (jon) this will cause problems with multiple nodes in same JVM
        FlumeNode node = FlumeNode.getInstance();

        // this makes the dfo present to the when reporting on the FlumeNode
        String dfonode = context.getValue(LogicalNodeContext.C_LOGICAL);
        DiskFailoverManager dfoman = node.getAddDFOManager(dfonode);

        return new DiskFailoverDeco<EventSink>(null, dfoman, new TimeTrigger(
            new ProcessTagger(), delayMillis), checkmillis);
      }
    };
  }

  @Override
  public String getName() {
    return "DiskFailover";
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    ReportEvent walRpt = dfoMan.getReport();
    rpt.merge(walRpt);
    ReportEvent sinkReport = sink.getReport();
    rpt.hierarchicalMerge(getName(), sinkReport);

    return rpt;
  }
}
