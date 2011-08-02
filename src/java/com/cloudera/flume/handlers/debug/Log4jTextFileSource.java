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

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.Event.Priority;
import com.google.common.base.Preconditions;

/**
 * This adds a parser that breaks out parts of a log4j log file and tries to
 * re-concatenate split lines.
 * 
 * TODO (jon) check if this is the hadoop log4j default out of the box.
 */
public class Log4jTextFileSource extends TextFileSource {

  public Log4jTextFileSource(String fname) {
    super(fname);
  }

  // This should parse a log4j line
  Pattern l4jPat = Pattern
      .compile("^(\\S+?) (\\S+?) (\\d\\d/\\d\\d/\\d\\d \\d\\d:\\d\\d:\\d\\d) (\\S+?) (.*)");

  DateFormat dformat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
  String prevLine = null;

  /**
   * Reads lines until we either reach the end of a file, or a line that looks
   * like the beginning of a new entry. If last line prevLine is set to null. If
   * it is a new event, we make the prev line contain that value and return the
   * completed event.
   */
  Event readUntilNextEvent(Matcher m) throws IOException {
    try {
      // new event!
      String host = m.group(1);
      // String xx = m.group(2); // TODO (jon) I don't know what this is
      Date d;
      d = dformat.parse(m.group(3));
      String prio = m.group(4);
      String body = m.group(5);

      // this is really just to differentiate entries at the "same time" at
      // second/millisecond resolution"
      long nanos = System.nanoTime();
      StringBuilder builder = new StringBuilder(body);
      while (true) {
        String s2 = raf.readLine();
        if (s2 == null) {
          // reached the last line
          prevLine = null;
          Event e = new EventImpl(builder.toString().getBytes(), d.getTime(), 
              Priority.valueOf(prio), nanos, host, new HashMap<String, byte[]>());
          return e;
        }

        // valid line, need to get more lines.
        Matcher m2 = l4jPat.matcher(s2);
        if (m2.matches()) {
          // a new matching line? event finished, save line for next round.
          Event e = new EventImpl(builder.toString().getBytes(), d.getTime(), 
              Priority.valueOf(prio), nanos, host, new HashMap<String, byte[]>());
          prevLine = s2;
          return e;
        }
        // didn't match, append to previous
        builder.append("\n");
        builder.append(s2);
      }
    } catch (ParseException e1) {
      // TODO XXX Auto-generated catch block
      e1.printStackTrace();
    }
    return null;
  }

  /**
   * Because of the semantics of next, we need to keep the previous line around.
   */
  public Event next() throws IOException {
    Preconditions.checkState(raf != null,
        "Need to open source before reading from it");
    String s = null;
    if (prevLine == null) {
      s = raf.readLine();

      if (s == null)
        return null; // last line

      // first line (fall through)

    } else {
      // event break
      s = prevLine;
    }

    Matcher m = l4jPat.matcher(s);
    if (m.matches()) {
      return readUntilNextEvent(m);
    }

    // This is unreachable.
    return null;
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        if (argv.length != 1) {
          throw new IllegalArgumentException("usage: log4jfile(filename) ");
        }
        return new Log4jTextFileSource(argv[0]);
      }

    };
  }
}
