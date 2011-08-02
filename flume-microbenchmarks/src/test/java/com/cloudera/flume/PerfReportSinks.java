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
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Benchmark;

/**
 * This set of performance tests isolate the system from I/O so so we can
 * measure the overhead of the actual reporting machinery.
 */
public class PerfReportSinks {

  @Test
  public void testNullSink() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    EventSink nullsnk = new NullSink();
    EventUtil.dumpAll(mem, nullsnk);
    b.mark("nullsink done");

    b.done();
  }

  @Test
  public void testCountSink() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    CounterSink snk = new CounterSink("counter");
    EventUtil.dumpAll(mem, snk);
    b.mark(snk.getName() + " done", snk.getCount());

    b.done();
  }

}
