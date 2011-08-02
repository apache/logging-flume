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
package com.cloudera.flume;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.sampler.IntervalSampler;
import com.cloudera.flume.reporter.sampler.ProbabilitySampler;
import com.cloudera.flume.reporter.sampler.ReservoirSamplerDeco;
import com.cloudera.util.Benchmark;

/**
 * Performance testing for the various samplers.
 */
public class PerfSamplers implements ExamplePerfData {

  @Test
  public void testReservoirSampler() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("Reservoir sampler + nullsink");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    b.mark("disk_loaded");

    NullSink nullsnk = new NullSink();
    EventUtil.dumpAll(mem, nullsnk);
    b.mark("warmup");

    int res_size[] = { 100, 1000, 10000, 100000 };
    for (int i = 0; i < res_size.length; i++) {
      mem.open();
      int sz = res_size[i];
      EventSink res = new ReservoirSamplerDeco<NullSink>(new NullSink(), sz);
      EventUtil.dumpAll(mem, res);
      b.mark("reservoir " + sz + " sampling done", sz);

      res.close();
      b.mark("sample dump done");
    }

    for (int i = 0; i < res_size.length; i++) {
      mem.open();
      int sz = res_size[i];
      CounterSink cnt = new CounterSink("null");
      EventSink res = new ReservoirSamplerDeco<CounterSink>(cnt, sz);
      EventUtil.dumpAll(mem, res);
      b.mark("reservoir", sz);

      res.close();
      b.mark("count", cnt.getCount());
    }
    b.done();
  }

  @Test
  public void testIntervalSampler() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("Interval sampler + nullsink");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    b.mark("disk_loaded");

    NullSink nullsnk = new NullSink();
    EventUtil.dumpAll(mem, nullsnk);
    b.mark("warmup");

    int interval_size[] = { 100000, 10000, 1000, 100 };
    for (int i = 0; i < interval_size.length; i++) {
      mem.open();
      int sz = interval_size[i];
      EventSink res = new IntervalSampler<NullSink>(new NullSink(), sz);
      EventUtil.dumpAll(mem, res);
      b.mark("interval " + sz + " sampling done", sz);

      res.close();
      b.mark("sample dump done");
    }

    for (int i = 0; i < interval_size.length; i++) {
      mem.open();
      int sz = interval_size[i];
      CounterSink cnt = new CounterSink("null");
      EventSink res = new IntervalSampler<CounterSink>(cnt, sz);
      EventUtil.dumpAll(mem, res);
      b.mark("interval", sz);

      res.close();
      b.mark("count", cnt.getCount());
    }
    b.done();
  }

  @Test
  public void testProbabilitySampler() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("Reservoir sampler + nullsink");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

    b.mark("disk_loaded");

    NullSink nullsnk = new NullSink();
    EventUtil.dumpAll(mem, nullsnk);
    b.mark("warmup");

    double probs[] = { .00001, .0001, .001, .01 };
    for (int i = 0; i < probs.length; i++) {
      mem.open();
      double prob = probs[i];
      EventSink res = new ProbabilitySampler<NullSink>(new NullSink(), prob);
      EventUtil.dumpAll(mem, res);
      b.mark("probability" + prob + " sampling done", prob);

      res.close();
      b.mark("sample dump done");
    }

    for (int i = 0; i < probs.length; i++) {
      mem.open();
      double prob = probs[i];
      CounterSink cnt = new CounterSink("null");
      EventSink res = new ProbabilitySampler<CounterSink>(cnt, prob);
      EventUtil.dumpAll(mem, res);
      b.mark("probability", prob);

      res.close();
      b.mark("count", cnt.getCount());
    }
    b.done();
  }

}
