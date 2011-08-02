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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This sink essentially acts as a pass through decorator, but will throw an
 * IOException every n'th append. Useful for testing FailOverSink or Stubborn
 * append sinks, or for fault tolerance testing.
 */
public class IntervalDroppyEventSink<S extends EventSink> extends
    EventSinkDecorator<S> {
  static final Logger LOG = LoggerFactory.getLogger(IntervalDroppyEventSink.class);

  int interval;
  int count = 0;

  public IntervalDroppyEventSink(S s, int i) {
    super(s);
    Preconditions.checkArgument(i > 0);
    this.interval = i;
  }

  @Override
  public void append(Event e) throws IOException {
    count++;
    if (count % interval == 0) {
      count = 0;
      // Dropped.
      LOG.info("Dropped event: " + e);
      return;
    }
    super.append(e);
  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "usage: intervalDroppy(interval)");
        int interval = Integer.parseInt(argv[0]);
        return new IntervalDroppyEventSink<EventSink>(null, interval);
      }

    };
  }

}
