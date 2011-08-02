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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;
import com.google.common.base.Preconditions;

/**
 * This injects events tags that are inserted in the data stream that mark the
 * beginning and end of different data streams. The a corresponding
 * BenchmarkReport decorator extracts these events and triggers the a start /
 * stop / or mark.
 * 
 * TODO (jon) maybe add a predicate expression of some sort and "mark" type
 */
public class BenchmarkInjectDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {
  static final Logger LOG = LoggerFactory
      .getLogger(BenchmarkInjectDecorator.class);
  final static AtomicInteger count = new AtomicInteger();

  public final static String ATTR_BENCHMARK = "BenchmarkInject";
  public final static String ATTR_BENCHMARK_TAG = "BenchmarkInjectTag";
  public final static byte[] BENCH_START = "start".getBytes(CharEncUtils.RAW);
  public final static byte[] BENCH_FIRST = "first".getBytes(CharEncUtils.RAW);
  public final static byte[] BENCH_STOP = "stop".getBytes(CharEncUtils.RAW);
  public final static byte[] BENCH_ERROR = "error".getBytes(CharEncUtils.RAW);
  final byte[] tag; // TODO convert this to byte[] to make it fast.

  AtomicBoolean first = new AtomicBoolean(true);

  public BenchmarkInjectDecorator(S s, String tag) {
    super(s);
    Preconditions.checkArgument(tag != null);
    this.tag = tag.getBytes();
  }

  public BenchmarkInjectDecorator(S s) {
    super(s);
    String t = NetUtils.localhost()
        + String.format("-%06d-", count.getAndIncrement()) + Clock.timeStamp();
    tag = t.getBytes();
  }

  /**
   * Exposed for testing
   */
  Event tagBench(Event e, byte[] evt) {
    e.set(ATTR_BENCHMARK_TAG, tag);
    e.set(ATTR_BENCHMARK, evt);
    return e;
  }

  @Override
  public void open() throws IOException, InterruptedException {
    super.open();
    super.append(tagBench(new EventImpl(new byte[0]), BENCH_START));
  }

  @Override
  public void close() throws IOException, InterruptedException {
    super.append(tagBench(getMetrics(), BENCH_STOP));
    super.close();

  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (first.getAndSet(false)) {
      super.append(tagBench(new EventImpl(new byte[0]), BENCH_FIRST));
    }
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0 || argv.length == 1,
            "usage: benchinject[(tag)]");
        String tag = (argv.length == 0) ? NetUtils.localhost()
            + String.format("-%06d-", count.getAndIncrement())
            + Clock.timeStamp() : argv[0];
        return new BenchmarkInjectDecorator<EventSink>(null, tag);
      }

    };
  }
}
