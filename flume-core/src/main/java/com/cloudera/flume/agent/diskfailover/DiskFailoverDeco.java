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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.DriverListener;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * This decorator does failover handling using the NaiveFileFailover mechanism.
 * It has a subordinate thread that drains the events that have been written to
 * disk. Latches are used to maintain open and close semantics.
 */
public class DiskFailoverDeco extends EventSinkDecorator<EventSink> {
  static final Logger LOG = LoggerFactory.getLogger(DiskFailoverDeco.class);

  final DiskFailoverManager dfoMan;
  final RollTrigger trigger;

  RollSink input;
  EventSource drainSource = null;
  Driver drainDriver;

  CountDownLatch drainCompleted = null; // block close until subthread is
  // completed
  CountDownLatch drainStarted = null; // blocks open until subthread is
  // started
  volatile IOException lastExn = null;

  final long checkmillis;
  final Context ctx;

  public DiskFailoverDeco(EventSink s, Context ctx,
      final DiskFailoverManager dfoman, RollTrigger t, long checkmillis) {
    super(new LazyOpenDecorator<EventSink>(s));
    this.ctx = ctx;
    this.dfoMan = dfoman;
    this.trigger = t;
    this.checkmillis = checkmillis;
  }

  public void setSink(EventSink sink) {
    this.sink = new LazyOpenDecorator<EventSink>(sink);
  }

  /**
   * TODO(jon): double check that the synchronization is appropriate here
   */
  @Override
  public synchronized void append(Event e) throws IOException,
      InterruptedException {
    Preconditions.checkNotNull(sink, "DiskFailoverDeco sink was invalid");
    Preconditions.checkArgument(isOpen.get(),
        "DiskFailoverDeco not open for append");

    if (lastExn != null) {
      throw lastExn;
    }

    input.append(e);
  }

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink,
        "Attempted to close a null DiskFailoverDeco subsink");
    LOG.debug("Closing DiskFailoverDeco");

    input.close(); // prevent new data from entering.
    dfoMan.close(); // put wal man into closing mode
    try {
      // wait for sub-thread to complete.
      LOG.debug("Waiting for subthread to complete .. ");
      int maxNoProgressTime = 10;

      ReportEvent rpt = sink.getMetrics();

      Long levts = rpt.getLongMetric(EventSink.Base.R_NUM_EVENTS);
      long evts = (levts == null) ? 0 : levts;
      int count = 0;
      while (true) {
        if (drainDriver.join(500)) {
          // driver successfully completed
          LOG.debug(".. subthread to completed");
          break;
        }

        // driver still running, did we make progress?
        ReportEvent rpt2 = sink.getMetrics();
        Long levts2 = rpt2.getLongMetric(EventSink.Base.R_NUM_EVENTS);
        long evts2 = (levts2 == null) ? 0 : levts;
        if (evts2 > evts) {
          // progress is being made, reset counts
          count = 0;
          evts = evts2;
          LOG.info("Closing disk failover log, subsink still making progress");
          continue;
        }

        // no progress.
        count++;
        LOG.info("Attempt " + count
            + " with no progress being made on disk failover subsink");
        if (count >= maxNoProgressTime) {
          LOG.warn("DFO drain thread was not making progress, forcing close");
          drainDriver.cancel();
          break;
        }
      }

    } catch (InterruptedException e) {
      LOG.error("Interrupted, DFO drain thread was not "
          + "making progress forcing close", e);
    }

    if (drainSource != null) {
      drainSource.close();
    }
    super.close();

    try {
      drainCompleted.await();
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
  synchronized public void open() throws IOException, InterruptedException {

    Preconditions.checkNotNull(sink,
        "Attepted to open a null DiskFailoverDeco subsink");
    Preconditions.checkState(!isOpen.get(), "EventSink Decorator was not open");

    LOG.debug("Opening DiskFailoverDeco");
    input = dfoMan.getEventSink(ctx, trigger);
    drainSource = dfoMan.getEventSource();

    dfoMan.open();
    dfoMan.recover();

    // TODO (jon) catch exceptions here and close them before rethrowing
    input.open();

    drainStarted = new CountDownLatch(1);
    drainCompleted = new CountDownLatch(1);

    drainDriver = new DirectDriver("FileFailover", drainSource, sink);
    /**
     * Don't synchronize on DiskFailoverDeco.this in the ConnectorListener
     * otherwise you might get a deadlock.
     */
    drainDriver.registerListener(new DriverListener.Base() {
      @Override
      public void fireStarted(Driver c) {
        drainStarted.countDown();
      }

      @Override
      public void fireStopped(Driver c) {
        drainCompleted.countDown();
      }

      @Override
      public void fireError(Driver c, Exception ex) {
        LOG.error("unexpected error with DiskFailoverDeco", ex);
        lastExn = (ex instanceof IOException) ? (IOException) ex
            : new IOException(ex.getMessage());
        try {
          drainDriver.getSource().close();
          drainDriver.getSink().close();
        } catch (Exception e) {
          LOG.error("Error closing", e);
        }
        drainCompleted.countDown();
        LOG.info("Error'ed Connector closed " + drainDriver);
      }
    });
    drainDriver.start();
    try {
      drainStarted.await();
    } catch (InterruptedException e) {
      LOG.error("Unexpected error waiting for drain to start", e);
      throw new IOException(e);
    }
    LOG.debug("Opened DiskFailoverDeco");
    isOpen.set(true);

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

        // TODO (jon) this will cause problems with multiple nodes in
        // same JVM
        FlumeNode node = FlumeNode.getInstance();

        // this makes the dfo present to the when reporting on the
        // FlumeNode
        String dfonode = context.getValue(LogicalNodeContext.C_LOGICAL);
        Preconditions.checkArgument(dfonode != null,
            "Context does not have a logical node name");
        DiskFailoverManager dfoman = node.getAddDFOManager(dfonode);

        return new DiskFailoverDeco(null, context, dfoman, new TimeTrigger(
            new ProcessTagger(), delayMillis), checkmillis);
      }
    };
  }

  @Override
  public String getName() {
    return "DiskFailover";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(sink.getName(), sink);
    map.put(dfoMan.getName(), dfoMan);
    if (drainSource != null) {
      // careful, drainSource can be null if deco not opened yet
      map.put("drainSource." + drainSource.getName(), drainSource);
    }

    return map;
  }

  public DiskFailoverManager getFailoverManager() {
    return dfoMan;
  }

  public RollSink getDFOWriter() {
    return input;
  }
}
