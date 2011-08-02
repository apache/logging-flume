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
package com.cloudera.flume.reporter.sampler;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;

/**
 * This simple sampler deterministically takes every nth event.
 * 
 * This sampler is not thread safe;
 * 
 * TODO(jon) make samplers just use EventSink instead of ReportingSink. Use
 * ReportManager to manage all ReportingSinks.
 */
public class IntervalSampler<S extends EventSink> extends EventSinkDecorator<S> {
  int interval;
  int count = 0;

  public IntervalSampler(S s, int inteval) {
    super(s);
    this.interval = inteval;
  }

  @Override
  public void append(Event e) throws IOException {

    if (count % interval == 0) {
      // forward
      getSink().append(e);
      count = 1;
      return;
    }

    count++;
    // drop message
  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        if (argv.length != 1) {
          throw new IllegalArgumentException(
              "usage: intervalSampler(intervalsize)");
        }
        int interval = Integer.parseInt(argv[0]);
        return new IntervalSampler<EventSink>(null, interval);
      }

    };
  }
}
