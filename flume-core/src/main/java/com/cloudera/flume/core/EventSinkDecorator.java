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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * A simple wrapper class that allows for easy extension of EventSinks. (This
 * acts like a trait/mixin)
 * 
 * The constructor can allow null sinks as arguments, but append/close/open
 * assumes non null.
 * 
 * The decorators have semantics where they act like synchronous functions. When
 * there is no buffering and close is just a synchronous call, this is simple,
 * just call the child close and return after it returns. If any buffering
 * occurs in the decorator, the decorator must flush its contents before
 * returning from close.
 */
public class EventSinkDecorator<S extends EventSink> extends EventSink.Base {

  protected S sink;
  protected AtomicBoolean isOpen = new AtomicBoolean(false);

  public EventSinkDecorator(S s) {
    // we allow null here to make FlumeBuilder implementation simpler
    this.sink = s;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink);
    Preconditions.checkState(isOpen.get(), "EventSink " + this.getName()
        + " not open");
    sink.append(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink);
    sink.close();
    isOpen.set(false);
  }

  @Override
  public void open() throws IOException, InterruptedException {
    Preconditions.checkNotNull(sink);
    Preconditions.checkState(!isOpen.get(), "EventSink Decorator was not open");
    sink.open();
    isOpen.set(true);
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    HashMap<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(sink.getName(), sink);
    return map;
  }

  @Deprecated
  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    Preconditions.checkNotNull(sink);
    super.getReports(namePrefix, reports);
    sink.getReports(namePrefix + getName() + ".", reports);
  }

  public S getSink() {
    return sink;
  }

  public void setSink(S sink) {
    this.sink = sink;
  }

  public static SinkDecoBuilder nullBuilder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0);
        return new EventSinkDecorator<EventSink>(null);
      }
    };
  }

}
