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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.reporter.ReportEvent;
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
  public void append(Event e) throws IOException;

  /**
   * This initializes a sink so that events can be appended. Events should only
   * be able to be opened once and will throw an IllegalStateException or
   * IOException if it is open is called and it is already opened.
   * 
   * Open is assumed to block until the sink is ready for append calls.
   */
  public void open() throws IOException;

  /**
   * This gracefully shuts down a sink. close will flush remaining events in the
   * sink memory.
   * 
   * If the data durable, close is allowed to exit if it will be recovered when
   * opened again.
   * 
   * Close will block until 1) all event appended to this sink have been flushed
   * and until 2) a subsequent open call should succeed (shared resource
   * situation).
   */
  public void close() throws IOException;

  /**
   * A do-nothing Sink that has default name (class name) and default report.
   * */
  public static class Base implements EventSink {
    @Override
    public void append(Event e) throws IOException {
    }

    @Override
    public void close() throws IOException {
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

  /**
   * This is an invalid sink that will instantiate but will always fail on use.
   * This can be used as a stub for sinks that get assigned later using let
   * expressions.
   * 
   * Examples where this is useful include autoBEChain, autoDFOChain and
   * autoE2EChain.
   */
  public static class StubSink extends Base {
    @Override
    public void append(Event e) throws IOException {
      throw new IOException("Attemping to append to a Stub Sink!");
    }

    @Override
    public void close() throws IOException {
      // this does not throw exception because close always succeeds.
    }

    @Override
    public void open() throws IOException {
      throw new IOException("Attemping to open a Stub Sink!");
    }

    /**
     * @return
     */
    public static SinkBuilder builder() {
      return new SinkBuilder() {
        @Override
        public EventSink build(Context context, String... argv) {
          return new StubSink();
        }
      };
    }
  }
}
