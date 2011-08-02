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
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.ReservoirSampler;
import com.google.common.base.Preconditions;

/**
 * This uses a reservoir sampling to choose with uniform probability the
 * specified number of events feed to this sink. When this sink is closed, it
 * flushes the current sample set through to the decorated sink.
 * 
 * This can be used in conjunction with a time-based sink (RollSink,
 * HistoryReporters, etc) to throttle the number of elements sampled in a given
 * amount of time in a single pass, without having to know the number of events
 * that occurred in a given time period. Specifically, the HistoryReporter would
 * decorate a ReservoirSamplerSink that in turn decorate an expensive filtering
 * or categorization function (e.g. histogramming based on java regex for
 * example).
 * 
 * NOTE: A side effect of the reservoir sampling is that the elements in the
 * sample will most likely be delivered out of order.
 */
public class ReservoirSamplerDeco<R extends EventSink> extends
    EventSinkDecorator<R> {

  final ReservoirSampler<Event> sampler;

  public ReservoirSamplerDeco(R snk, int samples) {
    super(snk);
    this.sampler = new ReservoirSampler<Event>(samples);
  }

  @Override
  public void close() throws IOException {
    flush();
    super.close();
  }

  public void flush() throws IOException {
    Preconditions.checkNotNull(sampler);
    List<Event> es = sampler.sample();
    for (Event e : es) {
      getSink().append(e);
    }
    sampler.clear();
  }

  @Override
  public void append(Event v) {
    sampler.onNext(v);
  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "usage: reservoirSampler(sample)");
        int sample = Integer.parseInt(argv[0]);
        return new ReservoirSamplerDeco<EventSink>(null, sample);
      }
    };
  }

}
