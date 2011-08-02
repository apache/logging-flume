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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This is for throughput tests. Here we put a series of events into memory then
 * them flush them when close is called.
 * 
 * This is similar to MemorySinkSource in functionality but is easier to
 * integrate with the initial single connector per process implementation.
 */
public class InMemoryDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  List<Event> evts = new ArrayList<Event>();

  public InMemoryDecorator(S s) {
    super(s);
  }

  @Override
  public void append(Event e) throws IOException {
    evts.add(e);
  }

  public List<Event> getEvents() {
    return Collections.unmodifiableList(evts);
  }

  @Override
  public void close() throws IOException {
    for (Event e : evts) {
      getSink().append(e);
    }

    evts = null;
    super.close();
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: inmem");
        return new InMemoryDecorator<EventSink>(null);
      }

    };
  }

}
