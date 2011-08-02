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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.MultipleIOException;

/**
 * This class will handles multiple input sources and makes a single output.
 * 
 * TODO (jon) actually implement the next() method. With 'let' statements this
 * may not be necessary -- instead we can write to a shared memory buffer
 * 
 * TODO (jon) THIS IS NOT COMPLETE, DO NOT USE
 */
public class FanInSource<S extends EventSource> extends EventSource.Base {
  final List<S> sinks = new CopyOnWriteArrayList<S>();

  public FanInSource() {
  }

  public FanInSource(Collection<? extends S> l) {
    sinks.addAll(l);
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
  public Event next() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    //TODO reports for all "sinks", see collectorsource
  }
  
}
