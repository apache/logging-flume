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

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * This provides a synchronous interface to getting events. An state/event-based
 * architecture will likely be more efficient than a thread based one because
 * there are many sources of information and only one output.
 */
public interface EventSource extends Reportable {

  /**
   * This is a blocking call that gets the next message from the source.
   * 
   * @return event or null if source is done/empty
   * @throws IOException
   */
  Event next() throws IOException, InterruptedException;

  public void open() throws IOException, InterruptedException;

  public void close() throws IOException, InterruptedException;

  /**
   * Generates one or more reports in some sort of readable format using the
   * supplied naming prefix.
   */
  public void getReports(String namePrefix, Map<String, ReportEvent> reports);

  /**
   * The stub source is a place holder that can get instantiated but never
   * should never have any calls made upon it. (opened/closed/next) . If that
   * ever happens it throws an exception.
   */
  public static class StubSource implements EventSource {

    @Override
    public void close() throws IOException {
      throw new IOException("Attempting to close a Stub Source!");
    }

    @Override
    public Event next() throws IOException {
      throw new IOException("Attempting to next a Stub Source!");
    }

    @Override
    public void open() throws IOException {
      throw new IOException("Attempting to open a Stub Source!");
    }

    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    @Override
    public ReportEvent getReport() {
      return new ReportEvent(getName());
    }

    public static SourceBuilder builder() {
      return new SourceBuilder() {
        @Override
        public EventSource build(String... argv) {
          return new StubSource();
        }
      };
    }

    /**
     * Bounded ranges on stubs sources.
     */
    public static SourceBuilder builder(final int minArgs, final int maxArgs) {
      return new SourceBuilder() {
        @Override
        public EventSource build(String... argv) {
          Preconditions.checkArgument(argv.length >= minArgs,
              "Too few arguments: expected at least " + minArgs
                  + " but only had " + argv.length);
          Preconditions.checkArgument(argv.length <= maxArgs,
              "Too many arguments : exepected at most " + maxArgs + " but had "
                  + argv.length);
          return new StubSource();
        }
      };
    }

    @Override
    public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
      reports.put(namePrefix + getName(), getReport());
    }
  }

  public static class Base implements EventSource {
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

    @Override
    public void close() throws IOException, InterruptedException {
    }

    @Override
    public Event next() throws IOException, InterruptedException {
      return null;
    }

    /**
     * This method should be called from sources which wish to track event
     * statistics.
     */
    synchronized protected void updateEventProcessingStats(Event e) {
      if (e == null)
        return;
      numBytes += e.getBody().length;
      numEvents++;
    }

    @Override
    public void open() throws IOException, InterruptedException {
    }

    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    @Override
    synchronized public ReportEvent getReport() {
      ReportEvent rpt = new ReportEvent(getName());

      rpt.setStringMetric(R_TYPE, getName());
      rpt.setLongMetric(R_NUM_BYTES, numBytes);
      rpt.setLongMetric(R_NUM_EVENTS, numEvents);

      return rpt;
    }

    @Override
    public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
      reports.put(namePrefix + getName(), getReport());
    }
  }

}
