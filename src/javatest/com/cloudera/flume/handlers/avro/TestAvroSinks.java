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

package com.cloudera.flume.handlers.avro;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.NetUtils;

/**
 * Tests AvroSinks and AvroSources. Pretty much mimics TestThriftSinks.
 */
public class TestAvroSinks implements ExampleData {
  public static Logger LOG = Logger.getLogger(TestAvroSinks.class);

  @Before
  public void setLogging() {
    Logger.getLogger(TestAvroSinks.class).setLevel(Level.DEBUG);
    Logger.getLogger(AvroEventSource.class).setLevel(Level.DEBUG);
  }

  @Before
  public void setLocalhost() {
    NetUtils.setLocalhost("host");
  }

  /**
   * The pipeline is:
   * 
   * text file -> mem
   * 
   * mem -> AvroEventSink -> AvroEventSource -> counter
   * 
   * @throws InterruptedException
   */
  @Test
  public void testAvroSend() throws IOException, InterruptedException {
    EventSource txt = new NoNlASCIISynthSource(25, 100);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();

    FlumeConfiguration conf = FlumeConfiguration.get();
    final AvroEventSource tes = new AvroEventSource(conf.getCollectorPort() + 1);
    tes.open();

    final CounterSink cnt = new CounterSink("count");
    cnt.open();
    Thread t = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, cnt);
        } catch (Exception e) {
        }
      }
    };
    t.start(); // drain the sink.

    // mem -> AvroEventSink
    AvroEventSink snk = new AvroEventSink("0.0.0.0",
        conf.getCollectorPort() + 1);
    snk.open();
    EventUtil.dumpAll(mem, snk);
    mem.close();
    snk.close();

    // a little delay to drain events at AvroEventSource queue
    try {
      Thread.sleep(1000);
      t.interrupt();
    } catch (InterruptedException e) {
    }
    tes.close();
    assertEquals(25, cnt.getCount());
    ReportEvent rpt = tes.getMetrics();
    /*
     * The check on BytesIn is different than one on TestThriftSinks tests. This
     * is because currently in the AvroSink version, BytesIn is equal to the
     * number of Bytes of the Event.body shipped.
     */
    assertEquals(2500, rpt.getLongMetric(AvroEventSource.A_BYTES_IN)
        .longValue());
    assertEquals(25, rpt.getLongMetric(AvroEventSource.A_DEQUEUED).longValue());
    assertEquals(25, rpt.getLongMetric(AvroEventSource.A_ENQUEUED).longValue());
    assertEquals(0, rpt.getLongMetric(AvroEventSource.A_QUEUE_CAPACITY)
        .intValue());
    assertEquals(1000, rpt.getLongMetric(AvroEventSource.A_QUEUE_FREE)
        .intValue());
  }

  /**
   * This tests starts many threads and confirms that the metrics values in
   * ThiftEventSource are consistently updated.
   * 
   * The pipeline is:
   * 
   * text file -> mem
   * 
   * mem -> AvroEventSink -> AvroEventSource -> counter
   */
  @Test
  public void testManyThreadsAvroSend() throws IOException,
      InterruptedException {
    final int threads = 10;
    final FlumeConfiguration conf = FlumeConfiguration.get();
    // this is a slight tweak to avoid port conflicts
    final AvroEventSource tes = new AvroEventSource(conf.getCollectorPort() + 1);
    tes.open();

    final CounterSink cnt = new CounterSink("count");
    cnt.open();
    Thread t = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, cnt);
        } catch (Exception e) {
        }
      }
    };
    t.start(); // drain the sink.

    // fork off threads threads and have them start all the same time.
    final CountDownLatch sendStarted = new CountDownLatch(threads);
    final CountDownLatch sendDone = new CountDownLatch(threads);
    final AtomicLong sendByteSum = new AtomicLong(0);
    for (int i = 0; i < threads; i++) {
      final int id = i;
      Thread th = new Thread() {
        public void run() {
          try {
            EventSource txt = new NoNlASCIISynthSource(25, 100);
            txt.open();
            MemorySinkSource mem = new MemorySinkSource();
            mem.open();
            EventUtil.dumpAll(txt, mem);
            txt.close();

            // mem -> AvroEventSink
            AvroEventSink snk = new AvroEventSink("0.0.0.0", conf
                .getCollectorPort() + 1);
            snk.open();

            sendStarted.countDown();
            sendStarted.await();
            EventUtil.dumpAll(mem, snk);
            mem.close();
            snk.close();

            sendByteSum.addAndGet(snk.sentBytes.get());
            LOG.info("sink " + id + " sent " + snk.sentBytes + " bytes");
            sendDone.countDown();

          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      th.start();
    }

    // wait for senders to send all
    sendDone.await();

    // a little delay get data to the receiving side.
    Thread.sleep(1000);

    tes.close();
    assertEquals(25 * threads, cnt.getCount());
    ReportEvent rpt = tes.getMetrics();
    assertEquals(2500 * threads, sendByteSum.get());
    assertEquals(2500 * threads, rpt.getLongMetric(AvroEventSource.A_BYTES_IN)
        .longValue());
    assertEquals(25 * threads, rpt.getLongMetric(AvroEventSource.A_DEQUEUED)
        .longValue());
    assertEquals(25 * threads, rpt.getLongMetric(AvroEventSource.A_ENQUEUED)
        .longValue());
    assertEquals(0, rpt.getLongMetric(AvroEventSource.A_QUEUE_CAPACITY)
        .longValue());
    assertEquals(1000, rpt.getLongMetric(AvroEventSource.A_QUEUE_FREE)
        .longValue());
  }

  /**
   * Checks to verify that a Avro server doesn't hang forever on closing
   */
  @Test
  public void testAvroEventServerCloseTimeout() throws IOException {
    final FlumeConfiguration conf = FlumeConfiguration.get();
    // this is a slight tweak to avoid port conflicts
    final AvroEventSource tes = new AvroEventSource(conf.getCollectorPort() + 1);
    tes.open();
    tes.enqueue(new EventImpl(new byte[0]));
    tes.close();
  }
}
