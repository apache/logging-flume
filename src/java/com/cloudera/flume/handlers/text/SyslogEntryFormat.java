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
package com.cloudera.flume.handlers.text;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This extracts values from a single text syslog line. The extract function
 * needs to have a year specified because the syslog format does not specify a
 * year!
 */
public class SyslogEntryFormat implements InputFormat, OutputFormat {
  final static Pattern SYSLOG_PAT = Pattern
      .compile("(\\S{3} \\d{1,2} \\d{2}:\\d{2}:\\d{2}) (\\S+) ([^:]*?)(:(.*))?");
  private static final String NAME = "syslog";

  // Not static because of concurrency bug in JDK on static DateFormats
  // see: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6231579
  final DateFormat SYSLOG_DF = new SimpleDateFormat("MMM dd HH:mm:ss");

  int year;

  public SyslogEntryFormat() {
    this(Calendar.getInstance().get(Calendar.YEAR));
  }

  public SyslogEntryFormat(int year) {
    this.year = year;
  }

  public Event extract(String s, int year) throws EventExtractException {
    Matcher m = SYSLOG_PAT.matcher(s);
    if (!m.matches())
      throw new EventExtractException("Does not match syslog format! " + s);

    String date = m.group(1);
    Date d = null;
    try {
      d = SYSLOG_DF.parse(date);
    } catch (ParseException e) {
      throw new EventExtractException("Invalid date format: " + date);
    }

    Calendar c = Calendar.getInstance();
    c.setTime(d);
    c.set(Calendar.YEAR, year);
    d = c.getTime();

    String host = m.group(2);

    // TODO(jon) body should be the raw entry, and a new field should be
    // created for this instead.
    String body = m.group(5); // TODO (jon) make this another field
    String service = m.group(3);
    if (body == null || body.length() == 0) {
//      body = service;
      service = null;
    }

    Map<String, byte[]> fields = new HashMap<String, byte[]>();
    if (service != null)
      fields.put("service", service.getBytes());
    // Event e = new EventImpl(body.getBytes(), d.getTime(), Priority.INFO,
    Event e = new EventImpl(s.getBytes(), d.getTime(), Priority.INFO, Clock
        .nanos(), host, fields);
    return e;

  }

  @Override
  public Event extract(String s) throws EventExtractException {
    return extract(s, this.year);
  }

  /**
   * This outputs a single line log entry similar to that generate by
   * syslog/syslog-ng.
   * 
   * It is generally in the form:
   * 
   * <date> <sourcehost> <service>: <message body>
   * 
   * Here is an example:
   * 
   * Aug 21 08:02:39 soundwave NetworkManager: <info> (wlan0): supplicant
   * 
   */
  private String format(Event e) {
    StringBuilder b = new StringBuilder();
    b.append(SYSLOG_DF.format(new Date(e.getTimestamp())));
    b.append(" ");
    b.append(e.getHost());
    b.append(" ");
    byte[] svc = e.get("service");
    if (svc != null) {
      b.append(new String(svc));
      b.append(": ");
    }
    b.append(new String(e.getBody()));
    b.append("\n");
    return b.toString();
  }

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    o.write(format(e).getBytes());
  }

  @Override
  public String getFormatName() {
    return NAME;
  }

  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {
      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length <= 1,
            "usage: syslogEntry[(year)]");
        int year = Calendar.getInstance().get(Calendar.YEAR);
        if (args.length >= 1) {
          year = Integer.parseInt(args[0]);
        }

        return new SyslogEntryFormat(year);
      }

      @Override
      public String getName() {
        return NAME;
      }
    };
  }

}
