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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This decorator reorders events. Give some yank probability, it stores values
 * from the stream, and the with some spew probability it reinserts events into
 * the stream.
 * 
 * This is single threaded and requires appends to reinsert values. Close will
 * flush the buffer and send the remaining the values.
 * 
 * This is basically for testing and generating out of order streams.
 */
public class ReorderDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  final List<Event> store = new ArrayList<Event>();

  final double yank; // probability an event will be yanked from the stream.
  final double spew; // probability that an event will be spewed back into the
  // stream.
  final Random rand;

  public ReorderDecorator(S s, double yank, double spew, long seed) {
    super(s);
    Preconditions.checkArgument(yank >= 0 && yank <= 1.0);
    Preconditions.checkArgument(spew >= 0 && spew <= 1.0);
    this.yank = yank;
    this.spew = spew;
    this.rand = new Random(seed);
  }

  public void append(Event e) throws IOException {
    if (rand.nextDouble() < yank) {
      store.add(e);
    } else {
      super.append(e);
    }

    if (!store.isEmpty()) {
      List<Event> removed = new ArrayList<Event>();
      for (Event e2 : store) {
        if (rand.nextDouble() < spew) {
          super.append(e2);
          removed.add(e2);
        }
      }

      store.removeAll(removed);
    }
  }

  public void close() throws IOException {
    if (!store.isEmpty()) {
      for (Event e : store) {
        super.append(e);
      }
      store.clear();
    }
    super.close();
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: reorder(delayProb, undelayProb)");
        double yank = Double.parseDouble(argv[0]);
        double spew = Double.parseDouble(argv[1]);
        return new ReorderDecorator<EventSink>(null, yank, spew, Clock
            .unixTime());
      }

    };
  }

}
