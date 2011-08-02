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

package com.cloudera.flume.handlers.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TSaneThreadPoolServer;
import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.NetUtils;

/**
 * Something broke in the performance benchmark so this is just a fast simple
 * functional test.
 */
public class TestThriftSinks implements ExampleData {
  public static Logger LOG = Logger.getLogger(TestThriftSinks.class);

  @Before
  public void setLogging() {
    Logger.getLogger(ThriftEventSource.class).setLevel(Level.DEBUG);
    Logger.getLogger(TestThriftSinks.class).setLevel(Level.DEBUG);
    Logger.getLogger(TSaneThreadPoolServer.class).setLevel(Level.DEBUG);
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
   * mem -> thriftEventSink -> thriftEventSource -> counter
   * 
   * @throws InterruptedException
   */
  @Test
  public void testThriftSend() throws IOException, InterruptedException {
    EventSource txt = new NoNlASCIISynthSource(25, 100);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(
        conf.getCollectorPort() + 1); // this is a slight
    // tweak to avoid port conflicts
    tes.open();

    final CounterSink cnt = new CounterSink("count");
    cnt.open();
    Thread t = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, cnt);
        } catch (IOException e) {
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    t.start(); // drain the sink.

    // mem -> ThriftEventSink
    ThriftEventSink snk = new ThriftEventSink("0.0.0.0", conf
        .getCollectorPort() + 1);
    snk.open();
    EventUtil.dumpAll(mem, snk);
    mem.close();
    snk.close();

    // a little delay to drain events at ThriftEventSource queue
    try {
      Thread.sleep(1000);
      t.interrupt();
    } catch (InterruptedException e) {
    }
    tes.close();
    assertEquals(25, cnt.getCount());
    ReportEvent rpt = tes.getMetrics();
    assertEquals(4475, rpt.getLongMetric(ThriftEventSource.A_BYTES_IN)
        .longValue());
    assertEquals(25, rpt.getLongMetric(ThriftEventSource.A_DEQUEUED)
        .longValue());
    assertEquals(25, rpt.getLongMetric(ThriftEventSource.A_ENQUEUED)
        .longValue());
    assertEquals(0, rpt.getLongMetric(ThriftEventSource.A_QUEUE_CAPACITY)
        .intValue());
    assertEquals(1000, rpt.getLongMetric(ThriftEventSource.A_QUEUE_FREE)
        .intValue());

  }

  @Test
  public void testOpenClose() throws IOException, InterruptedException {
    int port = FlumeConfiguration.get().getCollectorPort();
    final ThriftEventSource tes = new ThriftEventSource(port + 10);
    for (int i = 0; i < 50; i++) {
      LOG.info("ThirftEventSource open close attempt " + i);
      tes.open();
      tes.close();
    }
  }

  /**
   * This tests starts many threads and confirms that the metrics values in
   * ThiftEventSource are consistently updated.
   * 
   * The pipeline is:
   * 
   * text file -> mem
   * 
   * mem -> thriftEventSink -> thriftEventSource -> counter
   */
  @Test
  public void testManyThreadsThriftSend() throws IOException,
      InterruptedException {
    final int threads = 100;
    final FlumeConfiguration conf = FlumeConfiguration.get();
    // this is a slight tweak to avoid port conflicts
    final ThriftEventSource tes = new ThriftEventSource(
        conf.getCollectorPort() + 1);
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
            // TODO (jon) this may have different sizes due to the host it is
            // running on . Needs to be fixed.
            EventSource txt = new NoNlASCIISynthSource(25, 100);

            txt.open();
            MemorySinkSource mem = new MemorySinkSource();
            mem.open();
            EventUtil.dumpAll(txt, mem);
            txt.close();

            // mem -> ThriftEventSink
            ThriftEventSink snk = new ThriftEventSink("0.0.0.0", conf
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
    assertEquals(4475 * threads, sendByteSum.get());
    assertEquals(4475 * threads, rpt
        .getLongMetric(ThriftEventSource.A_BYTES_IN).longValue());
    assertEquals(25 * threads, rpt.getLongMetric(ThriftEventSource.A_DEQUEUED)
        .longValue());
    assertEquals(25 * threads, rpt.getLongMetric(ThriftEventSource.A_ENQUEUED)
        .longValue());
    assertEquals(0, rpt.getLongMetric(ThriftEventSource.A_QUEUE_CAPACITY)
        .longValue());
    assertEquals(1000, rpt.getLongMetric(ThriftEventSource.A_QUEUE_FREE)
        .longValue());

  }

  /**
   * Checks to verify that a thrift server doesn't hang forever on closing
   * 
   * @throws InterruptedException
   */
  @Test
  public void testThriftEventServerCloseTimeout() throws IOException,
      InterruptedException {
    final FlumeConfiguration conf = FlumeConfiguration.get();
    // this is a slight tweak to avoid port conflicts
    final ThriftEventSource tes = new ThriftEventSource(
        conf.getCollectorPort() + 1);
    tes.open();

    tes.enqueue(new EventImpl(new byte[0]));

    tes.close();

  }

  /**
   * Test insistent append metrics
   */
  @Test
  public void testThriftMetrics() throws JSONException, FlumeSpecException,
      IOException, InterruptedException {
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "thriftSink");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(ThriftEventSink.A_SENTBYTES));
    assertNotNull(rpt.getStringMetric(ThriftEventSink.A_SERVERHOST));
    assertNotNull(rpt.getLongMetric(ThriftEventSink.A_SERVERPORT));
  }

}
