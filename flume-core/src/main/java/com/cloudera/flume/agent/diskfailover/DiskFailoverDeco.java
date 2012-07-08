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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Driver.DriverState;
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

  private final DiskFailoverManager dfoMan;
  private final RollTrigger trigger;
  private final Context ctx;
  private final long checkMs;

  RollSink dfoProducer;
  EventSource dfoConsumer = null;
  DirectDriver dfoConsumerDriver;

  // volatile IOException lastExn = null;

  public DiskFailoverDeco(EventSink s, Context ctx,
      final DiskFailoverManager dfoman, RollTrigger t, long checkmillis) {
    super(new LazyOpenDecorator<EventSink>(s));
    this.ctx = ctx;
    this.dfoMan = dfoman;
    this.trigger = t;
    this.checkMs = checkmillis;
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
    dfoProducer.append(e);
  }

  /**
   * If the subthread has failed, fail by throwing the subthread's exception
   */
  void attemptToForwardException() throws IOException, InterruptedException {
    if (dfoConsumerDriver == null) {
      // if we fail during open before the driver is instantiated or started
      return;
    }
    Exception exn = dfoConsumerDriver.getException();
    if (exn != null) {
      if (exn instanceof IOException) {
        LOG.warn("wal consumer thread exited in error state: "
            + exn.getMessage());
        throw (IOException) exn;
      }
      if (exn instanceof InterruptedException) {
        LOG.warn("wal consumer thread exited in error due to interruption : "
            + exn.getMessage());
        throw (InterruptedException) exn;
      }
      LOG.error("Unexpected exception encountered! " + exn, exn);
    }
  }

  // This is going to go into the driver thread.
  void ensureClosedDrainDriver() throws IOException {
    LOG.debug("Ensuring wal consumer thread is done..");
    dfoMan.close(); // signal dfo source to stop (not blocking)
    if (dfoConsumerDriver == null) {
      // if we fail during open before the driver is instantiated or started
      return;
    }
    try {
      int maxNoProgressTime = 10;

      long evts = dfoConsumerDriver.appendCount;
      int count = 0;
      while (true) {
        if (dfoConsumerDriver.waitForAtLeastState(DriverState.IDLE, 500)) {
          // driver successfully completed
          LOG.debug(".. subthread to completed");
          break;
        }

        // driver still running, did we make progress?
        long evts2 = dfoConsumerDriver.appendCount;
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
          LOG.warn("WAL drain thread was not making progress, forcing close");
          dfoConsumerDriver.cancel();
          continue; // should exit with at least state ERROR
        }
      }

    } catch (InterruptedException e) {
      LOG.error("WAL drain thread interrupted", e);
    }
  }

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink,
        "Attempted to close a null DiskFailoverDeco subsink");
    if (!isOpen.get()) {
      LOG.warn("Attempt to double close DiskFailoverDec");
      return;
    }

    LOG.debug("Closing DiskFailoverDeco");
    if (dfoProducer != null) {
      dfoProducer.close();
    } else {
      LOG.warn("input in dfo deco was null!");
    }

    ensureClosedDrainDriver();
    isOpen.set(false);
    attemptToForwardException();
    LOG.debug("Closed DiskFailoverDeco");
  }

  @Override
  synchronized public void open() throws IOException, InterruptedException {

    Preconditions.checkNotNull(sink,
        "Attepted to open a null DiskFailoverDeco subsink");
    Preconditions.checkState(!isOpen.get(), "EventSink Decorator was not open");
    dfoMan.open();
    dfoMan.recover();

    LOG.debug("Opening DiskFailoverDeco");
    dfoProducer = dfoMan.getEventSink(ctx, trigger);
    dfoConsumer = dfoMan.getEventSource();

    dfoProducer.open();

    dfoConsumerDriver = new DirectDriver("FileFailover", dfoConsumer, sink);

    dfoConsumerDriver.start();
    boolean success = dfoConsumerDriver.waitForAtLeastState(
        DriverState.OPENING, 1000);
    if (!success) {
      dfoConsumerDriver.stop();
      attemptToForwardException();
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
    if (dfoConsumer != null) {
      // careful, drainSource can be null if deco not opened yet
      map.put("drainSource." + dfoConsumer.getName(), dfoConsumer);
    }

    return map;
  }

  public DiskFailoverManager getFailoverManager() {
    return dfoMan;
  }

  public RollSink getDFOWriter() {
    return dfoProducer;
  }
}
