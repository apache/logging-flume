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
package com.cloudera.flume.reporter.history;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This class uses a ScheduledExecutorService to rotate history entries every
 * maxAge milliseconds. We maintain up to maxHistory history data entries.
 * 
 * NOTE: Because this class uses the ScheduledExecutorService, the scheduled
 * history uses wall clock time and not a MockClock. To make this test run
 * relying on MockClock, I would need to modify and replace the
 * ScheduledExecutorService.
 * 
 * TODO (jon) Hack ScheduledExecutorService so that it uses MockClock instead of
 * system time.
 */
abstract public class ScheduledHistoryReporter<S extends EventSink> implements
    EventSink {

  // History is a list of timestamp and the old reports. New entries are
  // appended at the end, old entries are aged off of the head
  LinkedList<Pair<Long, S>> history;
  String name;
  S sink;
  Tagger tagger;
  long maxAgeMillis;
  long maxHistoryEntries;

  ScheduledExecutorService schedule = Executors
      .newSingleThreadScheduledExecutor();

  /**
   * Only used in a runnable for the event scheduler.
   * 
   * This is synchronized because the sink change must not be visible to
   * anything calling the sink interface.
   */
  synchronized private void rotate() {
    if (sink == null) {
      return;
    }

    try {
      sink.close();
      while (history.size() >= maxHistoryEntries) {
        history.removeFirst();

      }
      history.add(new Pair<Long, S>(tagger.getDate().getTime(), sink));
      sink = newSink(tagger);
      sink.open();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Right now this is exposed for testing purposes. Eventually rotate() may be
   * redefined to be used with a global scheduler to reduce the number of
   * threads in the system.
   */
  public void forcedRotate() {
    rotate();
  }

  public ScheduledHistoryReporter(String name, long maxAge, long maxHistory,
      Tagger t) {
    this.name = name;
    this.maxAgeMillis = maxAge;
    this.tagger = t;
    this.maxHistoryEntries = maxHistory;
    history = new LinkedList<Pair<Long, S>>();

    Runnable rotater = new Runnable() {
      public void run() {
        rotate();
      }
    };

    // Make initial delay the same as period -- this makes testing consistent.
    schedule
        .scheduleAtFixedRate(rotater, maxAge, maxAgeMillis, TimeUnit.MILLISECONDS);
  }

  abstract public S newSink(Tagger format) throws IOException;

  @Override
  public void append(Event e) throws IOException {
    synchronized (this) {
      Preconditions.checkNotNull(sink);
      sink.append(e);
    }
  }

  @Override
  synchronized public void close() throws IOException {
    sink.close();
    sink = null;
  }

  @Override
  synchronized public void open() throws IOException {
    if (sink == null) {
      sink = newSink(tagger);
      sink.open();
    }
  }

  @Override
  public String getName() {
    return name;
  }

  synchronized public List<Pair<Long, S>> getHistory() {
    return Collections.unmodifiableList(history);
  }

}
