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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This decorator sends a message multiple times.
 */
public class MultiplierDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {
  int multiplier;

  public MultiplierDecorator(S s, int mult) {
    super(s);
    this.multiplier = mult;
  }

  @Override
  public void append(Event e) throws IOException {
    // TODO (jon) make excludes generic.
    if (e.get(BenchmarkInjectDecorator.ATTR_BENCHMARK) != null) {
      super.append(e);
      return;
    }

    for (int i = 0; i < multiplier; i++) {
      // make a new instance for each event.
      super.append(new EventImpl(e));
    }
  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length < 2, "usage: repeat(init=1)");
        int mult = 1;
        if (argv.length >= 1)
          mult = Integer.parseInt(argv[0]);
        return new MultiplierDecorator<EventSink>(null, mult);
      }

    };
  }

}
