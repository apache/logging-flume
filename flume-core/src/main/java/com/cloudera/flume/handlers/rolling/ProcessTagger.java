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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.cloudera.util.Clock;

/**
 * This tagger uses process name and pid as its prefix to manage log write-ahead
 * files. It uses a file name convention and to tag on batches of events.
 */
public class ProcessTagger implements Tagger {

  private final static String DATE_FORMAT = "yyyyMMdd-HHmmssSSSZ";

  String lastTag;
  Date last;

  public ProcessTagger() {
  }

  @Override
  public String getTag() {
    return lastTag;
  }

  @Override
  public String newTag() {
    DateFormat dateFormat2 = new SimpleDateFormat(DATE_FORMAT);

    long pid = Thread.currentThread().getId();
    Date now = new Date(Clock.unixTime());
    long nanos = Clock.nanos();
    String f;
    // see: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6231579
    synchronized (dateFormat2) {
      f = dateFormat2.format(now);
    }

    // formatted so that lexigraphical and chronological can use same sort
    // yyyyMMdd-HHmmssSSSz.0000000nanos.00000pid
    lastTag = String.format("%s.%012d.%08d", f, nanos, pid);

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

}
