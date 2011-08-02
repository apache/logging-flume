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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.DriverListener;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.google.common.base.Preconditions;

/**
 * This connector hooks a source to a sink and allow this connection to be
 * stopped and started.
 * 
 * This assumes that sources and sinks are opened before the start() method is
 * called
 */
public class DirectDriver extends Driver {

  static final Logger LOG = LoggerFactory.getLogger(DirectDriver.class);

  EventSink sink;
  EventSource source;
  PumperThread thd;
  Exception error = null;
  NodeState state = NodeState.HELLO;
  final List<DriverListener> listeners = new ArrayList<DriverListener>();

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
        stopped = false;
        error = null;
        state = NodeState.ACTIVE;
        LOG.debug("Starting driver " + DirectDriver.this);
        fireStart();

        while (!stopped) {
          Event e = source.next();
          if (e == null)
            break;

          sink.append(e);
        }
      } catch (Exception e1) {
        // Catches all exceptions or throwables. This is a separate thread
        error = e1;
        stopped = true;

        LOG.error("Driving src/sink failed! " + DirectDriver.this + " because "
            + e1.getMessage(), e1);
        fireError(e1);
        state = NodeState.ERROR;
        return;
      }

      LOG.debug("Driver completed: " + DirectDriver.this);
      fireStop();
      state = NodeState.IDLE;
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

  public Exception getError() {
    return error;
  }

  @Override
  public NodeState getState() {
    return state;
  }

  /**
   * Callbacks cannot add or remove ConnectorListeners -- they can cause
   * deadlocks on the listeners lock if that happens.
   * 
   * Here we only lock on the 'listeners' object lock. Previously this locked on
   * the directconnector which could cause deadlocks with the callback.
   */
  @Override
  public void registerListener(DriverListener listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  @Override
  public void deregisterListener(DriverListener listener) {
    synchronized (listeners) {
      listeners.remove(listener);
    }
  }

  void fireStart() {
    synchronized (listeners) {
      for (DriverListener l : listeners) {
        l.fireStarted(this);
      }
    }
  }

  void fireStop() {
    synchronized (listeners) {
      for (DriverListener l : listeners) {
        l.fireStopped(this);
      }
    }
  }

  void fireError(Exception e) {
    synchronized (listeners) {
      for (DriverListener l : listeners) {
        l.fireError(this, e);
      }
    }
  }

  @Override
  public String toString() {
    return source.getClass().getSimpleName() + " | " + sink.getName();
  }
}
