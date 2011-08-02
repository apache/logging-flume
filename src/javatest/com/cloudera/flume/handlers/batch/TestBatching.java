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

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This tests batching/unbatching and gzip/gunzip compression
 */
public class TestBatching extends TestCase {

  public void testBatch() throws IOException {
    final int total = 104;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b =
        new BatchingDecorator<EventSink>(fo, 10, 0);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    b.close();
    assertEquals(11, cnt.getCount());

    // unbatch the batch.
    CounterSink cnt2 = new CounterSink("unbatch");
    UnbatchingDecorator<EventSink> ub =
        new UnbatchingDecorator<EventSink>(cnt2);
    Event ue = null;
    ub.open();
    while ((ue = mem.next()) != null) {
      ub.append(ue);
    }
    assertEquals(total, cnt2.getCount());
  }

  /**
   * Test that a timeout causes a batch to get committed.
   */
  public void testTimeout() throws IOException, InterruptedException {
    final int total = 100;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b =
        new BatchingDecorator<EventSink>(fo, 1024, 3000);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    Thread.sleep(5000);
    assertEquals(1, cnt.getCount());
    b.close();
  }

  /**
   * Test that close correctly flushes the remaining events, even if they don't
   * form an entire batch.
   */
  public void testCloseFlushes() throws IOException, InterruptedException {
    final int total = 102;
    // create a batch
    CounterSink cnt = new CounterSink("count");
    MemorySinkSource mem = new MemorySinkSource();
    FanOutSink<EventSink> fo = new FanOutSink<EventSink>(cnt, mem);
    BatchingDecorator<EventSink> b =
        new BatchingDecorator<EventSink>(fo, 10, 3000);
    b.open();
    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      b.append(e);
    }
    b.close();
    assertEquals(11, cnt.getCount());
  }

  public void testBatchBuilder() throws FlumeSpecException {
    String cfg = " { batch(10) => {unbatch => counter(\"cnt\") }}";
    @SuppressWarnings("unused")
    EventSink sink = FlumeBuilder.buildSink(new Context(), cfg);
  }

  public void testGzip() throws FlumeSpecException, IOException {

    MemorySinkSource mem = new MemorySinkSource();
    BatchingDecorator<EventSink> b =
        new BatchingDecorator<EventSink>(mem, 100, 0);
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

    System.out.printf("before: %d  gzip: %d  gunzip: %d\n", origsz, gzipsz,
        ungzsz);

    assertTrue(origsz > gzipsz); // got some benefit for compressing?
    assertEquals(origsz, ungzsz); // uncompress is same size as precompressed?

  }

  public void testGzipBuilder() throws FlumeSpecException {
    String cfg = " { gzip => {gunzip => counter(\"cnt\") }}";
    @SuppressWarnings("unused")
    EventSink sink = FlumeBuilder.buildSink(new Context(), cfg);

  }
}
