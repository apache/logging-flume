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
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.google.common.base.Preconditions;

/**
 * This source reads from a text file, takes the contents of each line and makes
 * it the body of an event. The current time is used as the timestamp and INFO
 * level priority.
 * 
 * This is great for direct comparison to 'grep | wc'
 * 
 * This version uses Buffered readers, hopefully this is faster.
 */
public class TextReaderSource extends EventSource.Base {
  static Logger LOG = Logger.getLogger(TextReaderSource.class);

  String fname;
  BufferedReader in;

  public TextReaderSource(String fname) {
    this.fname = fname;
  }

  public Event next() throws IOException {
    Preconditions.checkState(in != null,
        "Need to open source before reading from it");
    String s = in.readLine();
    if (s == null)
      return null;

    Event e = new EventImpl(s.getBytes());
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void close() throws IOException {
    in.close();
    LOG.info("File " + fname + " closed");
  }

  @Override
  public void open() throws IOException {
    this.in = new BufferedReader(new FileReader(fname));
    LOG.info("File " + fname + " opened");
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        if (argv.length != 1) {
          throw new IllegalArgumentException("usage: text(filename)");
        }

        return new TextReaderSource(argv[0]);
      }

    };
  }
}
