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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.Clock;
import com.cloudera.util.Pair;

/**
 * This is similar to the rolling event sink, but instead of writing to disk, it
 * caches old report information and generates a timeline report.
 * 
 * TODO (jon) unify this RollSink
 */
abstract public class HistoryReporter<S extends EventSink> extends
    EventSink.Base {

  // timestamp and the old report.
  LinkedList<Pair<Long, S>> history;
  String name;
  S sink;
  Tagger tagger;
  long maxAge;

  public HistoryReporter(String name, long maxAge, Tagger t) {
    this.name = name;
    this.maxAge = maxAge;
    this.tagger = t;
    history = new LinkedList<Pair<Long, S>>();
    this.tagger = t;
  }

  abstract public S newSink(Tagger format) throws IOException;

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (sink == null) {
      try {
        sink = newSink(tagger);
        sink.open();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }

    Date d = tagger.getDate();
    long delta = Clock.unixTime() - d.getTime();
    if (delta > maxAge) {
      try {
        sink.close();
        // save off old event sink
        history.add(new Pair<Long, S>(d.getTime(), sink));
        sink = newSink(tagger);
        sink.open();
      } catch (IOException e1) {
        // TODO This is an error condition that needs to be handled -- could be
        // due to resource exhaustion.
        e1.printStackTrace();
      }
    }

    sink.append(e);
    super.append(e);
  }

  @Override
  synchronized public void close() throws IOException, InterruptedException {
    sink.close();
    sink = null;
  }

  @Override
  public void open() throws IOException, InterruptedException {
    if (sink == null) {
      try {
        sink = newSink(tagger);
        sink.open();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }
  }

  @Override
  public String getName() {
    return name;
  }

  public List<Pair<Long, S>> getHistory() {
    return history;
  }

}
