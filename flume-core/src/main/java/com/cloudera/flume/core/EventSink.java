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
import java.util.Map;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;

/**
 * This is a consumer of events from some source.
 * 
 * Implementations of this interface must have a constructor with no arguments
 * and expose a bean (getter/setter) interface for configurable parameters.
 * 
 */
public interface EventSink extends Reportable {
  /**
   * This appends another event to the sink. It can throw two kinds of
   * exceptions IOExceptions and RuntimeExceptions (failed preconditions,
   * illegal state, etc)).
   */
  public void append(Event e) throws IOException, InterruptedException;

  /**
   * This initializes a sink so that events can be appended. Events should only
   * be able to be opened once and will throw an IllegalStateException or
   * IOException if it is open is called and it is already opened.
   * 
   * Open is assumed to block until the sink is ready for append calls.
   */
  public void open() throws IOException, InterruptedException;

  /**
   * This gracefully shuts down a sink. close will flush remaining events in the
   * sink memory.
   * 
   * If the data durable, close is allowed to exit if it will be recovered when
   * opened again.
   * 
   * Close will block until 1) all events appended to this sink have been
   * flushed and until 2) a subsequent open call should succeed (shared resource
   * situation).
   */
  public void close() throws IOException, InterruptedException;

  /**
   * Generate a simplified report. This only gathers a limited number of metrics
   * about the particular sink, and does not hierarchically gather information
   * from subsinks.
   */
  @Deprecated
  public ReportEvent getReport();

  /**
   * Generates one or more simplified reports in some sort of readable format
   * using the supplied naming prefix.
   */
  @Deprecated
  public void getReports(String namePrefix, Map<String, ReportEvent> reports);

  /**
   * A do-nothing Sink that has default name (class name) and default report.
   * */
  public static class Base implements EventSink {
    /** type attribute is common to all sinks */
    protected static final String R_TYPE = "type";
    /** byte count attribute is common to all sinks */
    protected static final String R_NUM_BYTES = "number of bytes";
    /** event count attribute is common to all sinks */
    protected static final String R_NUM_EVENTS = "number of events";

    /** total number of events appended to this sink */
    private long numEvents = 0;
    /** total number bytes appended to this sink */
    private long numBytes = 0;

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public void append(Event e) throws IOException,
        InterruptedException {
      updateAppendStats(e);
    }

    synchronized protected void updateAppendStats(Event e) {
      if (e == null)
        return;
      numBytes += e.getBody().length;
      numEvents++;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException, InterruptedException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws IOException, InterruptedException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public ReportEvent getMetrics() {
      return new ReportEvent(getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Reportable> getSubMetrics() {
      return ReportUtil.noChildren();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public ReportEvent getReport() {
      ReportEvent rpt = new ReportEvent(getName());

      rpt.setStringMetric(R_TYPE, getName());
      rpt.setLongMetric(R_NUM_BYTES, numBytes);
      rpt.setLongMetric(R_NUM_EVENTS, numEvents);

      return rpt;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
      reports.put(namePrefix + getName(), getReport());
    }
  }

  /**
   * This is an invalid sink that will instantiate but will always fail on use.
   * This can be used as a stub for sinks that get assigned later using let
   * expressions.
   * 
   * Examples where this is useful include autoBEChain, autoDFOChain and
   * autoE2EChain.
   */
  public static class StubSink extends Base {
    String name;

    public StubSink(String name) {
      this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
      return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void append(Event e) throws IOException, InterruptedException {
      throw new IOException("Attempting to append to a Stub Sink!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException, InterruptedException {
      // this does not throw exception because close always succeeds.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws IOException, InterruptedException {
      throw new IOException("Attempting to open a stub sink '" + getName()
          + "'!");
    }

    /**
     * @return
     */
    public static SinkBuilder builder(final String name) {
      return new SinkBuilder() {
        @Override
        public EventSink build(Context context, String... argv) {
          return new StubSink(name);
        }
      };
    }
  }
}
