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
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * This decorator does write ahead logging using the NaiveFileWAL mechanism. It
 * has a subordinate thread that drains the events that have been written to
 * disk. Latches are used to maintain open and close semantics.
 */
public class NaiveFileWALDeco extends EventSinkDecorator<EventSink> {
  static final Logger LOG = LoggerFactory.getLogger(NaiveFileWALDeco.class);

  final WALManager walman;
  final RollTrigger trigger;
  final AckListener queuer;
  final EventSinkDecorator<EventSink> drainSink;
  final Context ctx;
  final long checkMs;
  final AckListener al;

  RollSink walProducer = null;
  EventSource walConsumer = null;
  DirectDriver walConsumerDriver = null;

  public NaiveFileWALDeco(Context ctx, EventSink s, final WALManager walman,
      RollTrigger t, AckListener al, long checkMs) {
    super(s);
    this.ctx = ctx;
    this.walman = walman;
    this.trigger = t;
    this.queuer = new AckListener.Empty();
    this.al = al;
    // this.drainSink = new LazyOpenDecorator<EventSink>(
    // new AckChecksumRegisterer<EventSink>(s, al));
    // this.drainSink = new AckChecksumRegisterer<EventSink>(
    // new LazyOpenDecorator<EventSink>(s), al);
    this.drainSink = new AckChecksumRegisterer<EventSink>(s, al);

    this.checkMs = checkMs;
  }

  /**
   * When this decorator encounters a correct checksum pair, it add registers
   * the checksume to the specified ack listener.
   */
  public static class AckChecksumRegisterer<S extends EventSink> extends
      EventSinkDecorator<S> {
    AckListener q;

    public AckChecksumRegisterer(S s, AckListener q) {
      super(s);
      this.q = q;
    }

    @Override
    public void append(Event e) throws IOException, InterruptedException {
      super.append(e);

      // check for and attempt to register interest in tag's checksum
      byte[] acktype = e.get(AckChecksumInjector.ATTR_ACK_TYPE);

      if (acktype == null) {
        // nothing more to do
        return;
      }

      byte[] acktag = e.get(AckChecksumInjector.ATTR_ACK_TAG);

      if (Arrays.equals(acktype, AckChecksumInjector.CHECKSUM_STOP)) {
        String k = new String(acktag);
        LOG.debug("Registering interest in checksum group called '" + k + "'");
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
  public synchronized void append(Event e) throws IOException,
      InterruptedException {
    Preconditions.checkNotNull(sink, "NaiveFileWALDeco was invalid!");
    Preconditions.checkState(isOpen.get(),
        "NaiveFileWALDeco not open for append");
    attemptToForwardException();
    walProducer.append(e);
  }

  /**
   * If the subthread has failed, fail by throwing the subthread's exception
   */
  void attemptToForwardException() throws IOException, InterruptedException {
    if (walConsumerDriver == null) {
      // if we fail during open before the driver is instantiated or started
      return;
    }
    Exception exn = walConsumerDriver.getException();
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
    walman.stopDrains(); // signal wals source to stop (not blocking)
    if (walConsumerDriver == null) {
      // if we fail during open before the driver is instantiated or started
      return;
    }
    try {
      int maxNoProgressTime = 10;

      long evts = walConsumerDriver.appendCount;
      int count = 0;
      while (true) {
        if (walConsumerDriver.waitForAtLeastState(DriverState.IDLE, 500)) {
          // driver successfully completed
          LOG.debug(".. subthread to completed");
          break;
        }

        // driver still running, did we make progress?
        long evts2 = walConsumerDriver.appendCount;
        if (evts2 > evts) {
          // progress is being made, reset counts
          count = 0;
          evts = evts2;
          LOG.info("Closing wal log, subsink still making progress");
          continue;
        }

        // no progress.
        count++;
        LOG.info("Attempt " + count
            + " with no progress being made on wal consumer subsink");
        if (count >= maxNoProgressTime) {
          LOG.warn("WAL drain thread was not making progress, forcing close");
          walConsumerDriver.cancel();
          // walConsumerDriver.waitForAtLeastState(DriverState.IDLE, 5000);
          // break;
          continue; // should exit with atleast state ERROR
        }
      }

    } catch (InterruptedException e) {
      LOG.error("WAL drain thread interrupted", e);
    }
  }

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink,
        "Attmpted to close a null NaiveFileWALDeco");
    if (!isOpen.get()) {
      LOG.warn("Attempt to double close NaiveFileWALDeco");
      return;
    }

    LOG.debug("Closing NaiveFileWALDeco Source");
    if (walProducer != null) {
      walProducer.close(); // prevent new data from entering.
    } else {
      // TODO: how could this happen? things don't close in order?
      LOG.warn("input in naivefilewal deco was null!");
    }

    // Make sure subthread closed
    ensureClosedDrainDriver();

    isOpen.set(false);

    // This is throws an exception thrown by the subthread.
    attemptToForwardException();
    LOG.debug("Closed NaiveFileWALDeco");
  }

  @Override
  synchronized public void open() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink,
        "Attempted to open a null NaiveFileWALDeco subsink");
    LOG.debug("Opening NaiveFileWALDeco");
    walman.open();
    walman.recover();

    walProducer = walman.getAckingSink(ctx, trigger, queuer, checkMs);
    walConsumer = walman.getEventSource();

    // TODO (jon) catch exceptions here and close them before rethrowing
    walProducer.open();

    walConsumerDriver = new DirectDriver("naive file wal consumer",
        walConsumer, drainSink);
    walConsumerDriver.start();
    while (!walConsumerDriver.waitForAtLeastState(DriverState.ACTIVE, 1000)) {
      attemptToForwardException();
    }
    LOG.debug("Opened NaiveFileWALDeco");
    Preconditions.checkNotNull(sink);
    Preconditions.checkState(!isOpen.get());
    isOpen.set(true);
  }

  /**
   * This should only be set before open is called.
   */
  public synchronized void setSink(EventSink sink) {
    Preconditions.checkState(!isOpen.get(),
        "setSink should not be called once NaiveFileWALDeco is open");
    this.sink = sink;
    // this.drainSink.setSink(new LazyOpenDecorator<EventSink>(
    // new AckChecksumRegisterer<EventSink>(sink, al)));
    // this.drainSink.setSink(new AckChecksumRegisterer<EventSink>(sink, al));
    // this.drainSink.setSink(new LazyOpenDecorator<EventSink>(sink));
    this.drainSink.setSink(sink);

  }

  public synchronized boolean rotate() throws InterruptedException {
    return walProducer.rotate();
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
        Preconditions.checkArgument(walnode != null,
            "Context does not have a logical node name");

        long checkMs = 250; // TODO replace with config var;
        if (argv.length >= 3) {
          checkMs = Long.parseLong(argv[2]);
        }

        // TODO (jon) this is going to be unsafe because it creates before open.
        // This needs to be pushed into the logic of the decorator
        WALManager walman = node.getAddWALManager(walnode);
        return new NaiveFileWALDeco(context, null, walman, new TimeTrigger(
            delayMillis), node.getAckChecker().getAgentAckQueuer(), checkMs);
      }
    };
  }

  @Override
  public String getName() {
    return "NaiveFileWAL";
  }

  @Deprecated
  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    ReportEvent walRpt = walman.getMetrics();
    rpt.merge(walRpt);
    ReportEvent sinkReport = sink.getReport();
    rpt.hierarchicalMerge(getName(), sinkReport);
    return rpt;
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    return rpt;
  }

  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(walman.getName(), walman);
    map.put("drainSink." + sink.getName(), sink);
    if (walConsumer != null) {
      // careful, drainSource can be null if deco not opened yet
      map.put("drainSource." + walConsumer.getName(), walConsumer);
    }
    return map;
  }

}
