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
package com.cloudera.flume.handlers.rolling;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This rolling configurations based on a trigger such as a period of time or a
 * certain size of file. When a roll happens, the current instance of the
 * subordinate configuration is closed and a new instance of the sink is opened.
 */
public class RollSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(RollSink.class);

  final String fspec;
  EventSink curSink;
  final RollTrigger trigger;
  protected TriggerThread triggerThread = null;

  protected ExecutorService executor = null;

  private static int threadInitNumber = 0;
  final long checkLatencyMs; // default 4x a second
  private Context ctx; // roll context

  // reporting attributes and counters
  public final static String A_ROLLS = "rolls";
  public final static String A_ROLLFAILS = "rollfails";
  public final static String A_ROLLSPEC = "rollspec";
  public final String A_ROLL_TAG; // TODO (jon) parameterize this.
  public final static String DEFAULT_ROLL_TAG = "rolltag";

  final ReadWriteLock lock = new ReentrantReadWriteLock();

  final AtomicLong rolls = new AtomicLong();
  final AtomicLong rollfails = new AtomicLong();

  public RollSink(Context ctx, String spec, long maxAge, long checkMs) {
    this.ctx = ctx;
    A_ROLL_TAG = DEFAULT_ROLL_TAG;
    this.fspec = spec;
    this.trigger = new TimeTrigger(new ProcessTagger(), maxAge);
    this.checkLatencyMs = checkMs;
    LOG.info("Created RollSink: maxAge=" + maxAge + "ms trigger=[" + trigger
        + "] checkPeriodMs = " + checkLatencyMs + " spec='" + fspec + "'");
  }

  public RollSink(Context ctx, String spec, RollTrigger trigger, long checkMs) {
    this.ctx = ctx;
    A_ROLL_TAG = DEFAULT_ROLL_TAG;
    this.fspec = spec;
    this.trigger = trigger;
    this.checkLatencyMs = checkMs;
    LOG.info("Created RollSink: trigger=[" + trigger + "] checkPeriodMs = "
        + checkLatencyMs + " spec='" + fspec + "'");
  }

  private static synchronized int nextThreadNum() {
    return threadInitNumber++;
  }

  /**
   * This thread wakes up to check if a batch should be committed, no matter how
   * small it is.
   */
  class TriggerThread extends Thread {
    final CountDownLatch doneLatch = new CountDownLatch(1);
    final CountDownLatch startedLatch = new CountDownLatch(1);

    TriggerThread() {
      super("Roll-TriggerThread-" + nextThreadNum());
    }

    void doStart() {
      this.start();

      try {
        startedLatch.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for batch timeout thread to start");
      }
    }

    public void run() {
      startedLatch.countDown();
      try {
        while (!isInterrupted()) {
          // TODO there should probably be a lock on Roll sink but until we
          // handle
          // interruptions throughout the code, we cannot because this causes a
          // deadlock
          if (trigger.isTriggered()) {
            trigger.reset();

            LOG.debug("Rotate started by triggerthread... ");
            rotate();
            LOG.debug("Rotate stopped by triggerthread... ");
            continue;
          }

          try {
            Clock.sleep(checkLatencyMs);
          } catch (InterruptedException e) {
            LOG.debug("TriggerThread interrupted");
            doneLatch.countDown();
            return;
          }
        }
      } catch (InterruptedException e) {
        LOG.error("RollSink interrupted", e);
      }
      LOG.debug("TriggerThread shutdown");
      doneLatch.countDown();
    }
  };

  protected EventSink newSink(Context ctx) throws IOException {
    try {
      // TODO (jon) add roll-specific context information.
      return new CompositeSink(ctx, fspec);
    } catch (FlumeSpecException e) {
      // check done prior to construction.
      throw new IllegalArgumentException("This should never happen:"
          + e.getMessage());
    }
  };

  // currently it is assumed that there is only one thread handling appends, but
  // this has to be thread safe with respect to open and close (rotate) calls.
  Future<Void> future;

  // This is a large synchronized section. Won't fix until it becomes a problem.
  @Override
  public void append(final Event e) throws IOException, InterruptedException {
    Callable<Void> task = new Callable<Void>() {
      public Void call() throws Exception {
        synchronousAppend(e);
        return null;
      }
    };

    // keep track of the last future.
    future = executor.submit(task);
    try {
      future.get();
    } catch (ExecutionException e1) {
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        // we have a throwable that is not an exception.
        LOG.error("Got a throwable that is not an exception!", e1);
      }
    } catch (CancellationException ce) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(
          "Blocked append interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      Thread.currentThread().interrupt();
      throw (InterruptedException) ex;
    }
  }

  public void synchronousAppend(Event e) throws IOException,
      InterruptedException {
    Preconditions.checkState(curSink != null,
        "Attempted to append when rollsink not open");

    if (trigger.isTriggered()) {
      trigger.reset();
      LOG.debug("Rotate started by append... ");
      rotate();
      LOG.debug("... rotate completed by append.");
    }
    String tag = trigger.getTagger().getTag();

    e.set(A_ROLL_TAG, tag.getBytes());
    lock.readLock().lock();
    try {
      curSink.append(e);
      trigger.append(e);
      super.append(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * This method assumes it will be guarded by locks
   */
  boolean synchronousRotate() throws InterruptedException, IOException {
    rolls.incrementAndGet();
    if (curSink == null) {
      // wtf, was closed or never opened
      LOG.error("Attempting to rotate an already closed roller");
      return false;
    }
    try {
      curSink.close();
    } catch (IOException ioe) {
      // Eat this exception and just move to reopening
      LOG.warn("IOException when closing subsink",ioe);

      // other exceptions propagate out of here.
    }
    curSink = newSink(ctx);
    curSink.open();
    LOG.debug("rotated sink ");
    return true;
  }

  public boolean rotate() throws InterruptedException {
    while (!lock.writeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
      // interrupt the future on the other.
      if (future != null) {
        future.cancel(true);
      }

      // NOTE: there is no guarantee that this cancel actually succeeds.
    }
    // interrupted, lets go.
    try {
      synchronousRotate();
    } catch (IOException e1) {
      // TODO This is an error condition that needs to be handled -- could be
      // due to resource exhaustion.
      LOG.error("Failure when attempting to rotate and open new sink: "
          + e1.getMessage());
      rollfails.incrementAndGet();
      return false;
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public void close() throws IOException, InterruptedException {
    LOG.info("closing RollSink '" + fspec + "'");

    // attempt to get the lock, and if we cannot, issue a cancel
    while (!lock.writeLock().tryLock(1000, TimeUnit.MILLISECONDS)) {
      // interrupt the future on the other.
      if (future != null) {
        future.cancel(true);
      }
    }

    // we have the write lock now.
    try {
      if (executor != null) {
        executor.shutdown();
        executor = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
    // TODO triggerThread can race with an open call, but we really need to
    // avoid having the await while locked!
    if (triggerThread != null) {
      try {
        triggerThread.interrupt();
        triggerThread.doneLatch.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for batch timeout thread to finish");
        // TODO check finally
        throw e;
      }
    }

    lock.writeLock().lock();
    try {
      if (curSink == null) {
        LOG.info("double close '" + fspec + "'");
        return;
      }
      curSink.close();
      curSink = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void open() throws IOException, InterruptedException {
    lock.writeLock().lock();
    try {
      if (executor != null) {
        throw new IllegalStateException(
            "Attempting to open already open roll sink");
      }
      executor = Executors.newFixedThreadPool(1);

      Preconditions.checkState(curSink == null,
          "Attempting to open already open RollSink '" + fspec + "'");
      LOG.info("opening RollSink  '" + fspec + "'");
      trigger.getTagger().newTag();
      triggerThread = new TriggerThread();
      triggerThread.doStart();
    
      try {
        curSink = newSink(ctx);
        curSink.open();
      } catch (IOException e1) {
        LOG.warn("Failure when attempting to open initial sink", e1);
        executor.shutdown();
        executor = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getName() {
    return "Roll";
  }

  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setLongMetric(A_ROLLS, rolls.get());
    rpt.setLongMetric(A_ROLLFAILS, rollfails.get());
    rpt.setStringMetric(A_ROLLSPEC, fspec);
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    // subReports will handle case where curSink is null
    return ReportUtil.subReports(curSink);
  }

  public String getRollSpec() {
    return fspec;
  }

  @Deprecated
  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_ROLLS, rolls.get());
    rpt.setLongMetric(A_ROLLFAILS, rollfails.get());
    rpt.setStringMetric(A_ROLLSPEC, fspec);
    return rpt;
  }

  @Deprecated
  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    if (curSink != null) {
      curSink.getReports(namePrefix + getName() + ".", reports);
    }
  }

  /**
   * This is currently only used in tests.
   */
  @Deprecated
  public String getCurrentTag() {
    return trigger.getTagger().getTag();
  }

  /**
   * Builder for a spec based rolling sink. (most general version, does not
   * necessarily output to files!).
   */
  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length >= 2 && argv.length <= 3,
            "roll(rollmillis[, checkmillis]) { sink }");
        String spec = argv[0];
        long rollmillis = Long.parseLong(argv[1]);

        long checkmillis = 250; // TODO (jon) parameterize 250 argument.
        if (argv.length >= 3) {
          checkmillis = Long.parseLong(argv[2]);
        }

        try {
          // check sub spec to make sure it works.
          FlumeBuilder.buildSink(ctx, spec);

          // ok it worked, instantiate the roller
          return new RollSink(ctx, spec, rollmillis, checkmillis);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException("Failed to parse/build " + spec, e);
        }
      }
    };
  }
}
