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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.MultipleIOException;

/**
 * This class composes many sinks into a single sink. This version is
 * synchronous and the order event sinks are added controls the order they are
 * written.
 * 
 * TODO (jon) think about the semantics of partial failures in the multi sink.
 */
public class FanOutSink<S extends EventSink> extends EventSink.Base {
  // TODO (jon) not sure if I need the synchronized list wrapper around this or
  // not. Make a test to find out.
  final List<S> sinks = Collections
      .synchronizedList(new CopyOnWriteArrayList<S>());

  public FanOutSink() {
  }

  public FanOutSink(Collection<? extends S> l) {
    sinks.addAll(l);
  }

  public FanOutSink(S... ss) {
    sinks.addAll(Arrays.asList(ss));
  }

  public void add(S r) {
    sinks.add(r);
  }

  public void addAll(Collection<? extends S> c) {
    sinks.addAll(c);
  }

  public void drop(S r) {
    sinks.remove(r);
  }

  protected Iterable<S> iter() {
    return sinks;
  }

  /**
   * Close all children
   */
  @Override
  public void close() throws IOException {
    List<IOException> exs = new ArrayList<IOException>();

    for (S snk : sinks) {
      try {
        snk.close();
      } catch (IOException ioe) {
        exs.add(ioe);
      }
    }

    if (!exs.isEmpty()) {
      // From Hadoop 0.18.x api.
      throw MultipleIOException.createIOException(exs);
    }
  }

  /**
   * Open all children.
   */
  @Override
  public void open() throws IOException {
    List<IOException> exs = new ArrayList<IOException>();

    for (S snk : sinks) {
      try {
        snk.open();
      } catch (IOException ioe) {
        exs.add(ioe);
      }
    }

    if (!exs.isEmpty()) {
      // From Hadoop 0.18.x api.
      throw MultipleIOException.createIOException(exs);
    }
  }

  @Override
  synchronized public void append(Event e) throws IOException {
    List<IOException> exs = new ArrayList<IOException>();

    for (S snk : sinks) {
      try {
        snk.append(e);
        super.append(e);
      } catch (IOException ioe) {
        exs.add(ioe);
      }
    }

    if (!exs.isEmpty()) {
      // From Hadoop 0.18.x api.
      throw MultipleIOException.createIOException(exs);
    }
  }

  @Override
  public String getName() {
    return "Fanout";
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    int i = 0;
    for (EventSink s : sinks) {
      s.getReports(namePrefix + getName() + "." + i + ".", reports);
      i++;
    }
  }

}
