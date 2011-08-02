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

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;

/**
 * This provides a synchronous interface to getting events. An state/event-based
 * architecture will likely be more efficient than a thread based one because
 * there are many source of information and only one output
 */
public interface EventSource extends Reportable {

  /**
   * This is a blocking call that gets the next message from the source.
   * 
   * @return event or null if source is done/empty
   * @throws IOException
   */
  Event next() throws IOException;

  public void open() throws IOException;

  public void close() throws IOException;

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
  }

  public static class Base implements EventSource {
    @Override
    public void close() throws IOException {
    }

    @Override
    public Event next() throws IOException {
      return null;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public String getName() {
      return this.getClass().getSimpleName();
    }

    @Override
    public ReportEvent getReport() {
      return new ReportEvent(getName());
    }
  }
}
