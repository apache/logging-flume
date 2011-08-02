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
package com.cloudera.flume.agent.diskfailover;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.BenchmarkHarness;

/**
 * This check to make sure basic benchmarking should work -- ie when closed all
 * entries written to disk are flushed.
 */
public class TestDiskFailoverBenchmarking {

  @Test
  public void benchmarkBeforeFailover() throws FlumeSpecException, IOException,
      InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    // String spec =
    // "{ benchinject => { benchreport(\"pre\") =>  { diskFailover => [console, counter(\"beforecount\")] } } }";
    String spec = "{ benchinject => { benchreport(\"pre\") =>  { diskFailover => counter(\"beforecount\") } } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);

    EventSource src = MemorySinkSource.cannedData("test ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
        "beforecount");
    Assert.assertEquals(5, cnt.getCount());
    BenchmarkHarness.cleanupLocalWriteDir();

  }

  @Test
  public void benchmarkAfterFailover() throws FlumeSpecException, IOException,
      InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    String spec = "{ benchinject => { diskFailover => { benchreport(\"post\") =>  counter(\"beforecount\") } } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);

    EventSource src = MemorySinkSource.cannedData("test ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
        "beforecount");
    Assert.assertEquals(5, cnt.getCount());
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  @Test
  public void benchmarkBeforeWriteahead() throws FlumeSpecException,
      IOException, InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    String spec = "{ benchinject => { benchreport(\"pre\") =>  { diskFailover => counter(\"beforecount\") } } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);

    EventSource src = MemorySinkSource.cannedData("test ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
        "beforecount");
    Assert.assertEquals(5, cnt.getCount());
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  @Test
  public void benchmarkAfterWriteahead() throws FlumeSpecException,
      IOException, InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();

    String spec = "{ benchinject => { ackedWriteAhead => { benchreport(\"post\") =>  counter(\"beforecount\") } } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);

    EventSource src = MemorySinkSource.cannedData("test ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
        "beforecount");

    // +2 because of wal ack begin and end messages.
    Assert.assertEquals(5 + 2, cnt.getCount());
    BenchmarkHarness.cleanupLocalWriteDir();

  }
}
