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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CappedExponentialBackoff;
import com.cloudera.util.Clock;

/**
 * This tests the insistent opener (it tries many times before giving up)
 */
public class TestInsistentOpen {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestInsistentOpen.class);

  /**
   * Test that IOD retries the correct number of times when opening a sink that
   * fails twice and then succeeds.
   */
  @Test
  public void testInsistent() throws IOException, InterruptedException {
    // TODO(henry): this test relies on real clocks, and shouldn't. See below.
    EventSink fail2x = mock(EventSink.Base.class);
    // two exceptions then some success
    doThrow(new IOException("mock2")).doThrow(new IOException("mock"))
        .doNothing().when(fail2x).open();
    doReturn(new ReportEvent("stub")).when(fail2x).getMetrics();

    // max 5s, backoff initially at 10ms

    // WARNING! This test relies on being able to sleep for ~10ms and be woken
    // up three times in a 5s period. Seems plausible! But if you are looking at
    // this comment, it's probably because the test is failing on a loaded
    // machine...
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);
    InsistentOpenDecorator<EventSink> sink = new InsistentOpenDecorator<EventSink>(
        fail2x, bop);
    sink.open();
    sink.append(new EventImpl("test".getBytes()));
    sink.close();
    fail2x.getMetrics();

    ReportEvent rpt = sink.getMetrics();
    assertEquals(new Long(1), rpt
        .getLongMetric(InsistentOpenDecorator.A_REQUESTS));
    assertEquals(new Long(3), rpt
        .getLongMetric(InsistentOpenDecorator.A_ATTEMPTS));
    assertEquals(new Long(1), rpt
        .getLongMetric(InsistentOpenDecorator.A_SUCCESSES));
    assertEquals(new Long(2), rpt
        .getLongMetric(InsistentOpenDecorator.A_RETRIES));
    System.out.println(rpt.toText());
  }

  /**
   * Test that an IOD tries the correct number of times to reopen a failing
   * sink, then gives up.
   */
  @Test
  public void testInsistentRetry() {
    final MockClock m = new MockClock(0);

    EventSink failWhale = new EventSink.Base() {
      public ReportEvent getMetrics() {
        return new ReportEvent("failwhale-report");
      }

      @Override
      public void open() throws IOException {
        // Forward by 100ms, should cause IOD to try eleven times then give up
        m.forward(100);
        throw new IOException("fail open");
      }
    };

    Clock.setClock(m);
    // max 1s, backoff initially at 10ms, with a maxSingleSleep 1000ms and a
    // cumulative cap of 1000ms
    InsistentOpenDecorator<EventSink> sink = new InsistentOpenDecorator<EventSink>(
        failWhale, 1000, 10, 1000);

    try {
      sink.open();
    } catch (IOException e1) {

      ReportEvent rpt = sink.getMetrics();
      assertEquals(new Long(1), rpt
          .getLongMetric(InsistentOpenDecorator.A_REQUESTS));
      assertEquals(new Long(0), rpt
          .getLongMetric(InsistentOpenDecorator.A_SUCCESSES));

      // 11 attempts - one each at 100 * x for x in [0,1,2,3,4,5,6,7,8,9,10]
      // Retry trigger in IOD only fails when time > max, but passes when time =
      // max.
      assertEquals(new Long(11), rpt
          .getLongMetric(InsistentOpenDecorator.A_ATTEMPTS));
      assertEquals(new Long(11), rpt
          .getLongMetric(InsistentOpenDecorator.A_RETRIES));
      Clock.resetDefault();
      return; // success
    }
    fail("Ehr? Somehow the failwhale succeeded!");
  }

  /**
   * Tests to a thread cancel that forces a InterruptedException throw. Normally
   * an insistentOpen will never return if the subsink's open always fails.
   * interrupt forces an InterruptedException which the insistent open
   * translates into a IOException.
   * 
   * (Ideally it should propagate the InterruptedException, but I think that
   * change is pervasive and will wait for the next major version)
   */
  @Test
  public void testInsistentOpenCancel() throws IOException,
      InterruptedException {
    // TODO(henry): this test relies on real clocks, and shouldn't. See below.
    EventSink fail4eva = mock(EventSink.Base.class);
    // two exceptions then some success
    doThrow(new IOException("mock")).when(fail4eva).open();
    doReturn(new ReportEvent("stub")).when(fail4eva).getMetrics();

    final CountDownLatch done = new CountDownLatch(1);

    // max 5s, backoff initially at 10ms
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);
    final InsistentOpenDecorator<EventSink> sink = new InsistentOpenDecorator<EventSink>(
        fail4eva, bop);

    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          sink.open();
        } catch (IOException e) {
          // insistent translates interruptions into io exceptions
          done.countDown();
        }
      }
    };
    t.start();
    Clock.sleep(1000); // let the insistent open try a few times.
    t.interrupt(); // signal an interruption

    assertTrue("Timed out", done.await(1000, TimeUnit.MILLISECONDS));
  }

  /**
   * Test insistent open metrics
   */
  @Test
  public void testInsistentOpenMetrics() throws JSONException,
      FlumeSpecException, IOException, InterruptedException {
    ReportTestUtils.setupSinkFactory();

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "insistentOpen one");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(InsistentOpenDecorator.A_ATTEMPTS));
    assertNotNull(rpt.getLongMetric(InsistentOpenDecorator.A_GIVEUPS));
    assertNotNull(rpt.getLongMetric(InsistentOpenDecorator.A_REQUESTS));
    assertNotNull(rpt.getLongMetric(InsistentOpenDecorator.A_RETRIES));
    assertNotNull(rpt.getLongMetric(InsistentOpenDecorator.A_SUCCESSES));
    assertNotNull(rpt.getStringMetric("backoffPolicy.CappedExpBackoff.name"));
    assertEquals("One", rpt.getStringMetric("One.name"));

  }

}
