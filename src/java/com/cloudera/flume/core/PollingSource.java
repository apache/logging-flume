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
package com.cloudera.flume.core;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This periodically polls a pollable source of events, and then enqueues the
 * event for consumption. The Pollable should provide events without blocking.
 */
public class PollingSource extends EventSource.Base {
  static Logger LOG = Logger.getLogger(PollingSource.class);

  final Pollable src;
  final long period; // Time to wait if restart is true
  final BlockingQueue<Event> eventQueue =
      new ArrayBlockingQueue<Event>(FlumeConfiguration.get()
          .getPollerQueueSize());
  CountDownLatch started = null;
  CountDownLatch closed = null;
  volatile boolean shutdown = false;
  PollingThread poller;

  public static interface Pollable {
    Event poll();
  }

  public PollingSource(Pollable p, long period) {
    this.src = p;
    this.period = period;
  }

  /**
   * Polls an input and formats lines read as events, places them on the event
   * queue.
   */
  class PollingThread extends Thread {

    PollingThread() {
      super("PollingSource Thread");
    }

    /**
     * Periodically polls and returns an event.
     */
    public void run() {
      try {
        started.countDown();
        while (!shutdown) {
          Event e = src.poll();
          if (e != null) {
            LOG.debug(e);
            while (!eventQueue.offer(e, 200, TimeUnit.MILLISECONDS)) {
            }
          }

          Clock.sleep(period);
        }
      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn("PollingSource Thread received "
              + "unexpected InterruptedException", e);
        }
      }
      closed.countDown();
    }
  }

  public void close() throws IOException {
    // make sure
    shutdown = true;
    try {
      closed.await();

      // TODO (jon) this should block until the queue has been drained right?
    } catch (InterruptedException e) {
      LOG.debug("Waiting for pollable thread exit was interrupted", e);
    }
  }

  /**
   * Blocks on either getting an event from the queue or process exit (at which
   * point it throws an exception).
   */
  public Event next() throws IOException {
    Event evt = null;
    try {
      while (true) {

        evt = eventQueue.take();
        if (evt == null) {
          continue;
        }
        updateEventProcessingStats(evt);
        return evt;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("PollingSource was interrupted - " + e);
    }
  }

  /**
   * Open blocks until the poller thread is started and has made progress.
   */
  public void open() throws IOException {
    started = new CountDownLatch(1);
    closed = new CountDownLatch(1);
    poller = new PollingThread();
    poller.start();
    try {
      started.await();
    } catch (InterruptedException e) {
      LOG.error("Start has been interrupted", e);
      throw new IOException(e);
    }
  }

  /**
   * This is a source that periodically gets the report from the ReportManager
   */
  public static SourceBuilder reporterPollBuilder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length >= 0 && argv.length <= 1,
            "reportPoller[(periodMs)]");

        // period
        long period = FlumeConfiguration.get().getReporterPollPeriod();
        if (argv.length >= 1) {
          period = Integer.parseInt(argv[0]);
        }

        // poller
        Pollable p = new Pollable() {
          @Override
          public Event poll() {
            return FlumeNode.getInstance().getReport();
          }
        };

        return new PollingSource(p, period);
      }
    };
  }

}
