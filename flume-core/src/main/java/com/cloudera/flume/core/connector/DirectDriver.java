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
package com.cloudera.flume.core.connector;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This connector hooks a source to a sink and allow this connection to be
 * stopped and started.
 * 
 * This assumes that sources and sinks are closed and need to be opened.
 */
public class DirectDriver extends Driver {

  static final Logger LOG = LoggerFactory.getLogger(DirectDriver.class);

  final PumperThread thd;
  EventSink sink;// Guarded by object lock
  EventSource source; // Guarded by object lock

  Exception lastExn = null; // Guarded by statSignal
  DriverState state = DriverState.HELLO; // Guarded by stateSignal
  Object stateSignal = new Object(); // object do lock/notify on.

  // metrics
  public long nextCount = 0;
  public long appendCount = 0;

  public DirectDriver(EventSource src, EventSink snk) {
    this("pumper", src, snk);
  }

  public DirectDriver(String threadName, EventSource src, EventSink snk) {

    Preconditions.checkNotNull(src, "Driver Source was invalid");
    Preconditions.checkNotNull(snk, "Driver Sink was invalid");
    thd = new PumperThread(threadName);
    this.source = src;
    this.sink = snk;
  }

  class PumperThread extends Thread {
    volatile boolean stopped = true;

    public PumperThread(String name) {
      super();
      setName(name + "-" + getId());
    }

    public void run() {
      EventSink sink = null;
      EventSource source = null;
      synchronized (DirectDriver.this) {
        sink = DirectDriver.this.sink;
        source = DirectDriver.this.source;
      }
      try {
        synchronized (stateSignal) {
          state = DriverState.OPENING;
          stateSignal.notifyAll();
        }
        source.open();
        sink.open();
      } catch (Exception e) {
        // if open is interrupted or has an exception there was a problem.
        LOG.error("Closing down due to exception on open calls");
        errorCleanup(PumperThread.this.getName(), e);
        return;
      }

      synchronized (stateSignal) {
        lastExn = null;
        state = DriverState.ACTIVE;
        stateSignal.notifyAll();
      }

      LOG.debug("Starting driver " + DirectDriver.this);
      try {
        while (!stopped) {
          Event e = source.next();
          if (e == null)
            break;
          nextCount++;

          sink.append(e);
          appendCount++;
        }
      } catch (Exception e1) {
        // Catches all exceptions or throwables. This is a separate thread
        LOG.error("Closing down due to exception during append calls");
        errorCleanup(PumperThread.this.getName(), e1);
        return;
      }

      try {
        synchronized (stateSignal) {
          state = DriverState.CLOSING;
          stateSignal.notifyAll();
        }
        source.close();
        sink.close();
      } catch (Exception e) {
        LOG.error("Closing down due to exception during close calls");
        errorCleanup(PumperThread.this.getName(), e);
        return;
      }

      synchronized (stateSignal) {
        LOG.debug("Driver completed: " + DirectDriver.this);
        stopped = true;
        state = DriverState.IDLE;
      }
    }

    void ensureClosed(String nodeName) {
      try {
        getSource().close();
      } catch (IOException e) {
        LOG.error("Error closing " + nodeName + " source: " + e.getMessage());
      } catch (InterruptedException e) {
        // TODO reconsider this.
        LOG.error("Driver interrupted attempting to close source", e);
      }

      try {
        getSink().close();
      } catch (IOException e) {
        LOG.error("Error closing " + nodeName + " sink: " + e.getMessage());
      } catch (InterruptedException e) {
        // TODO reconsider this.
        LOG.error("Driver Interrupted attempting to close sink", e);
      }
    }

    void errorCleanup(String node, Exception ex) {
      LOG.info("Connector " + node + " exited with error: " + ex.getMessage(),
          ex);
      ensureClosed(node);
      synchronized (stateSignal) {
        lastExn = ex;
        stopped = true;
        LOG.error("Exiting driver " + node + " in error state "
            + DirectDriver.this + " because " + ex.getMessage());
        state = DriverState.ERROR;
        stateSignal.notifyAll();
      }
    }
  }

  @Override
  synchronized public void setSink(EventSink snk) {
    this.sink = snk;
  }

  synchronized public EventSink getSink() {
    return sink;
  }

  @Override
  synchronized public void setSource(EventSource src) {
    this.source = src;
  }

  synchronized public EventSource getSource() {
    return source;
  }

  @Override
  public synchronized void start() throws IOException {
    // don't allow thread to be "started twice"
    if (thd.stopped) {
      thd.stopped = false;
      thd.start();
    }
  }

  public synchronized boolean isStopped() {
    return thd.stopped;
  }

  @Override
  public synchronized void stop() throws IOException {
    thd.stopped = true;
  }

  /**
   * Start the mean shutdown.
   */
  public void cancel() {
    thd.interrupt();
  }

  @Override
  public void join() throws InterruptedException {
    join(0);
  }

  @Override
  public boolean join(long ms) throws InterruptedException {
    final PumperThread t = thd;
    t.join(ms);
    return !t.isAlive();
  }

  /**
   * return the last exception that caused driver to exit
   */
  public Exception getException() {
    return lastExn;
  }

  @Override
  public DriverState getState() {
    return state;
  }

  @Override
  public String toString() {
    return source.getClass().getSimpleName() + " | " + sink.getName();
  }

  /**
   * Wait up to millis ms for driver state to reach specified state. return true
   * if reached, return false if not.
   */
  @Override
  public boolean waitForState(DriverState state, long millis)
      throws InterruptedException {
    long now = Clock.unixTime();
    long deadline = now + millis;
    synchronized (stateSignal) {
      DriverState curState = this.state;
      //
      while (deadline > now) {
        curState = this.state;
        if (curState.equals(state)) {
          return true;
        }
        // still wrong state? wait more.
        now = Clock.unixTime();
        long waitMs = Math.max(0, deadline - now); // guarentee non neg
        if (waitMs == 0) {
          LOG.warn("Expected " + state + " but timed out in state " + curState);
          return false;
        }
        stateSignal.wait(waitMs);
      }
      // give up and return false
      LOG.warn("Expected " + state + " but timed out in state " + curState);
      return false;
    }
  }

  /**
   * Wait up to millis ms for driver state to reach at least the specified state
   * where HELLO < OPENING < ACTIVE < CLOSING < IDLE < ERROR
   * 
   * return true if reached, return false if not.
   */
  @Override
  public boolean waitForAtLeastState(DriverState state, long millis)
      throws InterruptedException {
    long now = Clock.unixTime();
    long deadline = now + millis;
    synchronized (stateSignal) {
      DriverState curState = this.state;
      while (deadline > now) {
        curState = this.state;
        if (state.ordinal() == curState.ordinal()) {
          return true;
        }
        if (state.ordinal() <= curState.ordinal()) {
          LOG.warn("Expected " + state + " but already in state " + curState);
          return true;
        }

        // still wrong state? wait more.
        now = Clock.unixTime();
        long waitMs = Math.max(0, deadline - now); // guarentee non neg
        if (waitMs == 0) {
          continue;
        }
        stateSignal.wait(waitMs);
      }
      // give up and return false
      LOG.error("Expected " + state + " but timed out in state " + curState);
      return false;
    }
  }

}
