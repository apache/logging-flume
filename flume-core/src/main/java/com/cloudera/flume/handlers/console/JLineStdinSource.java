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
package com.cloudera.flume.handlers.console;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.ConsoleOperations;
import jline.ConsoleReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This stdin source is a properly behaving source. It can be closed by a
 * different thread, and acts as if it is non-blocking.
 * 
 * The normal StdinSource that uses System.in.readLine() only has a blocking
 * mode. It does not return if in the readLine call with no incoming data.
 * 
 * Here we use jline's readline which acts character by character. To close
 * the jline readline, we interpose a extra check on read() that will return
 * CTRL_D (EOF) and allow a pending readline to exit.
 * 
 * Here's a link to the jline source
 * http://jline.git.sourceforge.net/git/gitweb.cgi?p=jline/jline;a=tree;f=src/
 * main/java/jline;hb=HEAD
 */
public class JLineStdinSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(JLineStdinSource.class);
  final AtomicBoolean opened = new AtomicBoolean(false);
  ConsoleReader rd;

  public JLineStdinSource() {
  }

  /**
   * This takes a input stream (stdin) and interposes on the read call. The
   * jline console will exit a readline if a CTRL_D (EOF) is "read" from the
   * source, so we add a boolean variable that allows us to force a CTRL_D into
   * the input stream.
   */
  static class ClosableInputStream extends InputStream {
    final InputStream in;
    final AtomicBoolean opened;

    ClosableInputStream(InputStream in, AtomicBoolean opened) {
      this.in = in;
      this.opened = opened;
    }

    @Override
    public int read() throws IOException {
      // have set this to poll..
      while (true) {
        // there is data buffered, read and return it.
        if (in.available() > 0)
          return in.read();

        // no data left
        if (!opened.get()) {
          // we are closed, return EOF
          return ConsoleOperations.CTRL_D;
        }
        try {
          // still open, wait a little and try again.
          Clock.sleep(50);
        } catch (InterruptedException e) {
          // interrupted? return end of file.
          return ConsoleOperations.CTRL_D;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing stdin source");

    boolean wasOpen = opened.getAndSet(false);
    if (!wasOpen) {
      LOG.warn("Double close on Stdin Char Source");
    }
    rd = null;
  }

  @Override
  public Event next() throws IOException {
    String s = null;

    Preconditions.checkState(rd != null, "Next on unopened sink!");

    s = rd.readLine();
    if (s == null) {
      return null; // end of stream
    }
    Event e = new EventImpl(s.getBytes(CharEncUtils.RAW));
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(rd == null && !opened.get());
    LOG.info("Opening stdin source");
    opened.set(true);
    InputStream is = new ClosableInputStream(new FileInputStream(
        FileDescriptor.in), opened);
    Writer out = new PrintWriter(new OutputStreamWriter(System.out, System
        .getProperty("jline.WindowsTerminal.output.encoding", System
            .getProperty("file.encoding"))));
    rd = new ConsoleReader(is, out);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        return new JLineStdinSource();
      }
    };
  }
}
