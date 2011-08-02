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
package com.cloudera.flume.handlers.rolling;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.core.Event;
import com.cloudera.util.Clock;

/**
 * This tagger uses process name and pid as its prefix to manage log write-ahead
 * files. It uses a file name convention and to tag on batches of events.
 */
public class ProcessTagger implements Tagger {
  private final static Pattern p = Pattern
      .compile(".*\\.(\\d+)\\.(\\d+-\\d+-\\d+)\\.seq");
  private final static String DATE_FORMAT = "yyyyMMdd-HHmmssSSSZ";

  /**
   * These event attributes names are used to keep some group information on
   * events.
   */
  public final static String A_TID = "tid";
  public final static String A_EXE = "exe";

  String lastTag;
  Date last;

  String exe;
  long pid;

  public ProcessTagger() {
  }

  public String createTag(String name, int pid) {
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    String f;
    f = dateFormat.format(new Date(Clock.unixTime()));

    if (name.length() > 200) {
      name = name.substring(0, 200); // concatenate long prefixes
    }

    String fname = String.format("%s.%08d.%s.seq", name, pid, f);
    return fname;
  }

  public static Date extractDate(String s) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    if (s == null)
      return null;

    Matcher m = p.matcher(s);

    if (!m.find()) {
      return null;
    }
    String date = m.group(2);

    Date d = dateFormat.parse(date, new ParsePosition(0));
    return d;
  }

  @Override
  public String getTag() {
    return lastTag;
  }

  @Override
  public String newTag() {
    DateFormat dateFormat2 = new SimpleDateFormat(DATE_FORMAT);

    long pid = Thread.currentThread().getId();
    String prefix = "log";
    Date now = new Date(Clock.unixTime());
    long nanos = Clock.nanos();
    String f;
    // see: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6231579
    synchronized (dateFormat2) {
      f = dateFormat2.format(now);
    }
    if (prefix.length() > 200) {
      prefix = prefix.substring(0, 200); // concatenate long prefixes
    }

    lastTag = String.format("%s.%08d.%s.%012d.seq", prefix, pid, f, nanos);

    this.pid = pid;
    this.exe = prefix;
    this.last = now;
    return lastTag;
  }

  @Override
  public Date getDate() {
    if (last == null) {
      return new Date(0);
    }
    return new Date(last.getTime()); // Defensive copy
  }

  @Override
  public void annotate(Event e) {
    // ByteBuffer one liners are to convert longs to byte[8]s.
    e.set(A_TID, ByteBuffer.allocate(8).putLong(pid).array());
    e.set(A_EXE, exe.getBytes());
    e.set(A_TXID, ByteBuffer.allocate(8).putLong(last.getTime()).array());
  }
}
