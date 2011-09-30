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
package com.cloudera.flume.handlers.rolling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

public class TestRollSink {
  public static final Logger LOG = LoggerFactory.getLogger(TestRollSink.class);

  @Before
  public void setDebug() {
    // log4j specific debugging level
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  /**
   * Tests that the rolling event sink correctly tags the output filename.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testEscapedFilenameCloseFlushes() throws IOException,
      InterruptedException {
    Tagger tagger = new ProcessTagger() {
      @Override
      public String getTag() {
        return "-testtag";
      }

      @Override
      public String newTag() {
        return "-testtag";
      }
    };
    final File f = FileUtil.mktempdir();
    RollSink snk = new RollSink(new Context(), "test", new TimeTrigger(tagger,
        10000), 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink("file:///" + f.getPath(),
            "sub-%{service}%{rolltag}");
      }
    };

    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();
    File fo = new File(f.getPath() + "/sub-foo-testtag");
    assertTrue(fo.exists());
    FileUtil.rmr(f);
  }

  /**
   * Tests that the rolling event sink correctly tags the output filename.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testEscapedFilename() throws IOException, InterruptedException {
    Tagger tagger = new ProcessTagger() {
      @Override
      public String getTag() {
        return "-testtag";
      }

      @Override
      public String newTag() {
        return "-testtag";
      }
    };
    final File f = FileUtil.mktempdir();
    RollSink snk = new RollSink(new Context(), "escapedCustomDfs(\"file:///"
        + f.getPath() + "\", \"sub-%{service}%{rolltag}\")", new TimeTrigger(
        tagger, 10000), 250);

    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();
    File fo = new File(f.getPath() + "/sub-foo-testtag");
    assertTrue(fo.exists());
    FileUtil.rmr(f);
  }

  @Test
  public void testAutoRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        2000, 10000); // two
    // second sleeper, but check period is really long

    Event e = new EventImpl("this is a test message".getBytes());
    snk.open();
    snk.append(e);
    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("foo");
    Clock.sleep(3000); // sleep 3s

    // the roller automatically flushed!
    assertEquals(1, cnt.getCount());
    snk.close();
  }

  /**
   * This verifies that the roller's trigger works multiple times, and at about
   * the right frequency.
   */
  @Test
  public void testMultiTimedRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        200, 100);
    // 200 ms auto forced roll threshold.

    snk.open();
    Clock.sleep(100); // sleep until about 100 ms; no flush yet.
    assertEquals(Long.valueOf(0),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush
    assertEquals(Long.valueOf(1),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush.
    assertEquals(Long.valueOf(2),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    Clock.sleep(200); // auto flush.
    assertEquals(Long.valueOf(3),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));
    snk.close();
  }

  /**
   * This verifies that the roller's trigger works multiple times, and at about
   * the right frequency.
   */
  @Test
  public void testMultiCountRoll() throws IOException, InterruptedException {
    RollSink snk = new RollSink(new ReportTestingContext(), "counter(\"foo\")",
        new SizeTrigger(10, new ProcessTagger()), 100);
    // every 10 body-bytes we should roll

    snk.open();

    assertEquals(Long.valueOf(0),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    // a 10 byte body
    Event e = new EventImpl("0123456789".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(1),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    // 5 bytes (no trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(1),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));
    // 5 more bytes (ok trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(2),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    // 27 bytes but only on trigger
    e = new EventImpl("012345678901234567890123456".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(3),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    // 5 bytes (no trigger)
    e = new EventImpl("01234".getBytes());
    snk.append(e);
    Clock.sleep(200); // at least one check period
    assertEquals(Long.valueOf(3),
        snk.getMetrics().getLongMetric(RollSink.A_ROLLS));

    snk.close();
  }

  /**
   * Test metrics
   * 
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testGetRollMetrics() throws JSONException, FlumeSpecException,
      IOException, InterruptedException {
    ReportTestUtils.setupSinkFactory();

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "roll(100) { one } ");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLFAILS));
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLS));
    assertEquals("one", rpt.getStringMetric(RollSink.A_ROLLSPEC));
    assertNull(rpt.getStringMetric("One.name"));

    // need to open to have sub sink show up
    snk.open();

    ReportEvent all = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(all).toString());
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLFAILS));
    assertNotNull(rpt.getLongMetric(RollSink.A_ROLLS));
    assertEquals("one", rpt.getStringMetric(RollSink.A_ROLLSPEC));
    assertEquals("One", all.getStringMetric("One.name"));

    snk.close();
  }

  /**
   * This test has an append that is blocked because a sink inside of a roll
   * always throws an exception. (ex: a down network connection). This test
   * attempts to guarantee that the rotate succeeds in a reasoanble amoutn of
   * time.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRollAppendBlockedDeadlock() throws IOException,
      InterruptedException {
    final EventSink mock = mock(EventSink.Base.class);
    doNothing().doThrow(new IOException()).when(mock).close();
    final Event e1 = new EventImpl("foo1".getBytes());
    final Event e2 = new EventImpl("foo2".getBytes());
    doThrow(new IOException()).when(mock).append(e1);
    doThrow(new IOException()).when(mock).append(e2);

    SinkFactoryImpl sfi = new SinkFactoryImpl();
    sfi.setSink("rollLock", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return mock;
      }
    });
    FlumeBuilder.setSinkFactory(sfi);

    final RollSink roll = new RollSink(LogicalNodeContext.testingContext(),
        "insistentOpen stubbornAppend insistentAppend rollLock", 1000000,
        1000000);
    final CountDownLatch latch = new CountDownLatch(1);
    // excessively long roll and check times which allow test to force checks.
    Thread t = new Thread("blocked append thread") {
      public void run() {
        try {
          roll.open();
          roll.append(e1); // append blocks.
        } catch (InterruptedException e) {
          latch.countDown();
          LOG.error("Exited with expected Exception");
          return;
        } catch (IOException e) {
          LOG.info("Got the unexpected IOException exit", e);
          e.printStackTrace();
        }
        LOG.info("Got the unexpected clean exit");
      }
    };
    t.start();
    // have another thread timeout.
    final CountDownLatch latch2 = new CountDownLatch(1);
    Thread t2 = new Thread("roll rotate thread") {
      public void run() {
        try {
          Clock.sleep(1000); // this is imperfect, but wait for append to block.
          roll.rotate();
          roll.close();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          latch2.countDown();
        }
      }
    };
    t2.start();
    boolean success = latch.await(5, TimeUnit.SECONDS);
    assertTrue(success);

    success = latch2.await(5, TimeUnit.SECONDS);
    assertTrue(success);
  }

  @Test
  public void testTriggerKWArg() throws FlumeSpecException {
    FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(1000, trigger=time) { null }");
    FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(1000, trigger=size(1000)) { null }");
  }

  @Test(expected = FlumeSpecException.class)
  public void testBadTriggerKWArgType() throws FlumeSpecException {
    FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(1000, trigger=foo) { null }");
  }

  @Test(expected = FlumeSpecException.class)
  public void testBadMissingTimeTriggerKWArg() throws FlumeSpecException {
    FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(trigger=size(100)) { null }");
  }

  @Test(expected = FlumeSpecException.class)
  public void testBadTriggerKWArgArg() throws FlumeSpecException {
    FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(1000, trigger=size(\"badarg\")) { null }");
  }

  @Test
  public void testSizeTriggerFunctional() throws FlumeSpecException,
      IOException, InterruptedException {
    // a ridiculous amount of time trigger time to forces size trigger
    EventSink snk = FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "roll(1000000, trigger=size(10)) { console }");
    snk.open();

    // Events from this loop:
    // 0, 1, trigger, 2, 3, trigger, 4, 5, trigger, 6, 7, trigger, 8 ,9,
    for (int i = 0; i < 10; i++) {
      snk.append(new EventImpl("6chars".getBytes()));
    }
    snk.close();

    ReportEvent rpt = snk.getMetrics();
    // See above for why there are 4 triggers:
    assertEquals(4, (long) rpt.getLongMetric(RollSink.A_ROLLS));
  }

  /**
   * This verifies that when roller's trigger aborts, the sink closes correctly
   */
  @Test(timeout=30000)
  public void testTriggerAborted() throws IOException,
      InterruptedException {

    Tagger tagger = new ProcessTagger() {
      @Override
      public String getTag() {
        return "-testtag";
      }

      @Override
      public String newTag() {
        // throw exception from the Trigger Thread
        if (Thread.currentThread().getName().contains("Roll-TriggerThread"))
          throw new RuntimeException("testExp");
        return "-testtag";
      }
    };

    final File f = FileUtil.mktempdir();
    RollSink snk = new RollSink(new Context(), "test", new TimeTrigger(tagger,
        10000), 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink("file:///" + f.getPath(),
            "sub-%{service}%{rolltag}");
      }
    };

    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    // wait for the trigger thread to abort down
    while (snk.triggerThread.isAlive()) {
      Clock.sleep(100);
    }
    snk.close();
    // verify that the trigger thread did the cleanup
    assertTrue(snk.triggerThread.doneLatch.getCount() == 0);
    File fo = new File(f.getPath() + "/sub-foo-testtag");
    assertTrue(fo.exists());
    FileUtil.rmr(f);
  }

}
