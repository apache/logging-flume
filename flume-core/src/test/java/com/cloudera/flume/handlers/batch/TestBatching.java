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
package com.cloudera.flume.handlers.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This tests batching/unbatching and gzip/gunzip compression
 */
public class TestBatching {
  public static final Logger LOG = Logger.getLogger(TestBatching.class);

  @Test
  public void testBatch() throws IOException, InterruptedException {
    final int total = 104;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b = new BatchingDecorator<EventSink>(fo, 10, 0);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    b.close();
    Assert.assertEquals(11, cnt.getCount());

    // unbatch the batch.
    CounterSink cnt2 = new CounterSink("unbatch");
    UnbatchingDecorator<EventSink> ub = new UnbatchingDecorator<EventSink>(cnt2);
    Event ue = null;
    ub.open();
    while ((ue = mem.next()) != null) {
      ub.append(ue);
    }
    Assert.assertEquals(total, cnt2.getCount());
  }

  /**
   * Test that a timeout causes a batch to get committed.
   */
  @Test
  public void testTimeout() throws IOException, InterruptedException {
    final int total = 100;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b = new BatchingDecorator<EventSink>(fo, 1024,
        3000);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    Thread.sleep(5000);
    Assert.assertEquals(1, cnt.getCount());
    b.close();
  }

  /**
   * Test that close correctly flushes the remaining events, even if they don't
   * form an entire batch.
   */
  @Test
  public void testCloseFlushes() throws IOException, InterruptedException {
    final int total = 102;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b = new BatchingDecorator<EventSink>(fo, 10,
        3000);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    b.close();
    Assert.assertEquals(11, cnt.getCount());
  }

  @Test
  public void testBatchBuilder() throws FlumeSpecException {
    String cfg = " { batch(10) => {unbatch => counter(\"cnt\") }}";
    @SuppressWarnings("unused")
    EventSink sink = FlumeBuilder.buildSink(new Context(), cfg);
  }

  @Test
  public void testGzip() throws FlumeSpecException, IOException,
      InterruptedException {

    MemorySinkSource mem = new MemorySinkSource();
    BatchingDecorator<EventSink> b = new BatchingDecorator<EventSink>(mem, 10,
        0);
    b.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("canned data " + i).getBytes());
      b.append(e);
    }

    MemorySinkSource mem2 = new MemorySinkSource();
    GzipDecorator<EventSink> gz = new GzipDecorator<EventSink>(mem2);
    Event be = mem.next();
    gz.open();
    gz.append(be);

    Event gzbe = mem2.next();
    MemorySinkSource mem3 = new MemorySinkSource();
    GunzipDecorator<EventSink> gunz = new GunzipDecorator<EventSink>(mem3);
    gunz.open();
    gunz.append(gzbe);
    Event gunze = mem3.next();

    int origsz = new WriteableEvent(be).toBytes().length;
    int gzipsz = new WriteableEvent(gzbe).toBytes().length;
    int ungzsz = new WriteableEvent(gunze).toBytes().length;

    LOG.info(String.format("before: %d  gzip: %d  gunzip: %d", origsz, gzipsz,
        ungzsz));

    assertTrue(origsz > gzipsz); // got some benefit for compressing?
    assertEquals(origsz, ungzsz); // uncompress is same size as
    // precompressed?

  }

  @Test
  public void testGzipBuilder() throws FlumeSpecException {
    String cfg = " { gzip => {gunzip => counter(\"cnt\") }}";
    @SuppressWarnings("unused")
    EventSink sink = FlumeBuilder.buildSink(new Context(), cfg);

  }

  @Test
  public void testEmptyBatches() throws FlumeSpecException, IOException,
      InterruptedException {
    BatchingDecorator<EventSink> snk = new BatchingDecorator<EventSink>(
        new ConsoleEventSink(), 2, 100000);
    snk.open();

    for (int i = 0; i < 10; i++) {
      if (i % 4 == 0) {
        snk.append(new EventImpl(("test " + i).getBytes()));

      }
      snk.endBatchTimeout();
    }
    snk.close();

    ReportEvent rpt = snk.getMetrics();
    LOG.info(rpt.toString());
    assertEquals(0, (long) rpt.getLongMetric(BatchingDecorator.R_FILLED));
    assertEquals(8, (long) rpt.getLongMetric(BatchingDecorator.R_EMPTY));
    // Extra trigger happens on close
    assertEquals(11, (long) rpt.getLongMetric(BatchingDecorator.R_TRIGGERS));
  }

  @Test
  public void testBatchingMetrics() throws IOException, InterruptedException {
    EventSource src = MemorySinkSource.cannedData("this is a data", 200);
    MemorySinkSource mem = new MemorySinkSource();
    UnbatchingDecorator<EventSink> unbatch = new UnbatchingDecorator<EventSink>(
        mem);
    GunzipDecorator<EventSink> gunzip = new GunzipDecorator<EventSink>(unbatch);

    int evts = 5;
    int latency = 0; // 0 never triggers on time
    GzipDecorator<EventSink> gzip = new GzipDecorator<EventSink>(gunzip);
    BatchingDecorator<EventSink> batch = new BatchingDecorator<EventSink>(gzip,
        evts, latency);

    src.open();
    batch.open();
    EventUtil.dumpAll(src, batch);
    src.close();
    batch.close();

    // check metrics.
    ReportEvent ubRpt = unbatch.getMetrics();
    assertEquals(40, (long) ubRpt
        .getLongMetric(UnbatchingDecorator.R_BATCHED_IN));
    assertEquals(200, (long) ubRpt
        .getLongMetric(UnbatchingDecorator.R_BATCHED_OUT));
    assertEquals(0, (long) ubRpt
        .getLongMetric(UnbatchingDecorator.R_PASSTHROUGH));

    ReportEvent bRpt = batch.getMetrics();
    assertEquals(1, (long) bRpt.getLongMetric(BatchingDecorator.R_EMPTY));
    assertEquals(40, (long) bRpt.getLongMetric(BatchingDecorator.R_FILLED));
    assertEquals(0, (long) bRpt.getLongMetric(BatchingDecorator.R_TIMEOUTS));
    assertEquals(41, (long) bRpt.getLongMetric(BatchingDecorator.R_TRIGGERS));

    ReportEvent gzr = gzip.getMetrics();
    assertTrue(gzr.getLongMetric(GzipDecorator.R_GZIPSIZE) < gzr
        .getLongMetric(GzipDecorator.R_EVENTSIZE));
    assertEquals(40, (long) gzr.getLongMetric(GzipDecorator.R_EVENTCOUNT));

    ReportEvent ugzr = gunzip.getMetrics();
    assertTrue(ugzr.getLongMetric(GunzipDecorator.R_GZIPSIZE) < ugzr
        .getLongMetric(GunzipDecorator.R_GUNZIPSIZE));
    assertEquals(40, (long) ugzr.getLongMetric(GunzipDecorator.R_GZIPCOUNT));
    assertEquals(0, (long) ugzr.getLongMetric(GunzipDecorator.R_PASSTHROUGH));
  }

}
