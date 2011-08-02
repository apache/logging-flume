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
package com.cloudera.flume.handlers.debug;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.CharEncUtils;
import com.google.common.base.Preconditions;

/**
 * Connects stdin as a source. Each line is a new event entry
 * 
 * Input does not get forwarded properly through the watchdog so it is disabled
 * by default when using it
 */
public class StdinSource extends EventSource.Base {
  final static Logger LOG = Logger.getLogger(StdinSource.class.getName());

  BufferedReader rd = null;

  public StdinSource() {
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing stdin source");
    // don't actually close stdin (because we won't be able to open it again)
    rd = null;
  }

  @Override
  public Event next() throws IOException {
    Preconditions.checkState(rd != null, "Next on unopened sink!");
    String s = rd.readLine();
    if (s == null) {
      return null; // end of stream
    }
    Event e = new EventImpl(s.getBytes(CharEncUtils.RAW));
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    LOG.info("Opening stdin source");
    if (rd != null) {
      throw new IllegalStateException("Stdin source was already open");
    }
    rd = new BufferedReader(new InputStreamReader(System.in));
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        return new StdinSource();
      }
    };
  }

}
