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
import java.util.HashMap;
import java.util.Map;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;

/**
 * This delays turns a sink into a lazy one by delaying opening a node until an
 * append attempt is made.
 * 
 * This is thread safe.
 */
public class LazyOpenSource<S extends EventSource> extends EventSource.Base {
  private boolean logicallyOpen = false;
  private boolean actuallyOpen = false;
  private S src;

  public LazyOpenSource(S s) {
    this.src = s;
  }

  @Override
  synchronized public void open() {
    logicallyOpen = true;
  }

  @Override
  public Event next() throws IOException, InterruptedException {
    synchronized (this) {
      if (logicallyOpen && !actuallyOpen) {
        src.open();
        actuallyOpen = true;
      }
    }
    return src.next();
  }

  @Override
  synchronized public void close() throws IOException, InterruptedException {
    actuallyOpen = false;
    logicallyOpen = false;
    src.close();

  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setStringMetric("actuallyOpen", "" + actuallyOpen);
    rpt.setStringMetric("logicallyOpen", "" + logicallyOpen);
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(src.getName(), src);
    return map;
  }
}
