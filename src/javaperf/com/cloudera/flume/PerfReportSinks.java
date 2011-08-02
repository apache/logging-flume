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

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.reporter.MultiReporter;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.builder.SimpleRegexReporterBuilder;
import com.cloudera.flume.reporter.histogram.RegexGroupHistogramSink;
import com.cloudera.util.Benchmark;

/**
 * This set of performance tests isolate the system from I/O so so we can
 * measure the overhead of the actual reporting machinery.
 */
public class PerfReportSinks implements ExamplePerfData {

  @Test
  public void testNullSink() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

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
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

    b.mark("disk_loaded");

    CounterSink snk = new CounterSink("counter");
    EventUtil.dumpAll(mem, snk);
    b.mark(snk.getName() + " done", snk.getCount());

    b.done();
  }

  @Test
  public void testHadoopRegexes() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("hadoop_regexes");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

    b.mark("disk_loaded");

    SimpleRegexReporterBuilder bld = new SimpleRegexReporterBuilder(
        HADOOP_REGEXES);

    Collection<RegexGroupHistogramSink> sinks = bld.load();
    MultiReporter snk = new MultiReporter("hadoop_regex_sinks", sinks);
    snk.open();
    b.mark("filters_loaded", new File(HADOOP_REGEXES).getName(), sinks.size());

    EventUtil.dumpAll(mem, snk);
    b.mark(snk.getName() + " done");

    b.done();
  }

  @Test
  public void testHadoopRegexes11() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("hadoop_regexes");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

    b.mark("disk_loaded");

    SimpleRegexReporterBuilder bld = new SimpleRegexReporterBuilder(
        HADOOP_REGEXES_11);

    Collection<RegexGroupHistogramSink> sinks = bld.load();
    MultiReporter snk = new MultiReporter("hadoop_regex_sinks", sinks);
    snk.open();
    b.mark("filters_loaded", new File(HADOOP_REGEXES_11).getName(), sinks
        .size());

    EventUtil.dumpAll(mem, snk);
    b.mark(snk.getName() + " done");

    b.done();
  }
}
