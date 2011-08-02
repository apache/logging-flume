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

package com.cloudera.flume.agent.durability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.FileUtil;

/**
 * This suite of tests bangs on the WAL with many concurrent writers
 */
public class TestNaiveFileWALManagerConcurrently {
  public static Logger LOG = Logger
      .getLogger(TestNaiveFileWALManagerConcurrently.class);

  @Before
  public void setDebug() {
    Logger.getLogger(NaiveFileWALManager.class).setLevel(Level.DEBUG);
  }

  /**
   * Test case exercises one wal manager, one ackedwriteahead deco, many
   * threads.
   */
  public void doSharedWALDeco(final int count, final int threads)
      throws IOException, InterruptedException, FlumeSpecException {
    File dir = FileUtil.mktempdir();
    final CountDownLatch start = new CountDownLatch(threads);
    final CountDownLatch done = new CountDownLatch(threads);
    final NaiveFileWALManager wal = new NaiveFileWALManager(dir);
    wal.open();

    Context ctx = new ReportTestingContext();
    EventSink cntsnk = new CompositeSink(ctx, "counter(\"total\")");
    // use the same wal, but different counter.
    final EventSink snk = new NaiveFileWALDeco(ctx, cntsnk, wal,
        new TimeTrigger(new ProcessTagger(), 1000000), new AckListener.Empty(),
        1000000);
    snk.open();

    for (int i = 0; i < threads; i++) {
      new Thread() {
        @Override
        public void run() {
          start.countDown();
          try {
            EventSource src = new NoNlASCIISynthSource(count, 100);
            start.await();
            src.open();
            EventUtil.dumpAll(src, snk);
            src.close();
          } catch (Exception e) {
            LOG.error("failure", e);
            // fail("e");
          } finally {
            done.countDown();
          }
        }

      }.start();
    }

    boolean ok = done.await(30, TimeUnit.SECONDS);
    assertTrue("Test timed out!", ok);
    Thread.sleep(1000);
    snk.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("total");
    long ci = cnt.getCount();
    LOG.info("count : " + ci);
    assertEquals((count * threads) + 2, ci);

    FileUtil.rmr(dir);
  }

  /**
   * Test case that exercises one shared walManager, many ackedwriteahead decos,
   * many threads
   */
  void doSharedWALManager(final int count, final int threads)
      throws IOException, InterruptedException {
    File dir = FileUtil.mktempdir();
    final CountDownLatch start = new CountDownLatch(threads);
    final CountDownLatch done = new CountDownLatch(threads);
    final NaiveFileWALManager wal = new NaiveFileWALManager(dir);
    wal.open();

    for (int i = 0; i < threads; i++) {
      final int idx = i;
      new Thread() {
        @Override
        public void run() {
          start.countDown();
          try {
            EventSource src = new NoNlASCIISynthSource(count, 100);
            Context ctx = new ReportTestingContext();
            EventSink snk = new CompositeSink(ctx, "counter(\"total." + idx
                + "\")");
            // use the same wal, but different counter.
            snk = new NaiveFileWALDeco(ctx, snk, wal, new TimeTrigger(
                new ProcessTagger(), 1000000), new AckListener.Empty(), 1000000);
            src.open();
            snk.open();

            start.await();

            EventUtil.dumpAll(src, snk);
            src.close();
            snk.close();
          } catch (Exception e) {
            LOG.error("failure", e);
            // fail("e");
          } finally {
            done.countDown();
          }
        }

      }.start();
    }

    boolean ok = done.await(30, TimeUnit.SECONDS);
    assertTrue("Test timed out!", ok);
    Thread.sleep(1000);

    long sum = 0;
    for (int i = 0; i < threads; i++) {

      CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
          "total." + i);
      long ci = cnt.getCount();
      LOG.info("count " + i + " : " + ci);
      sum += ci;

    }

    LOG.info("sum == " + sum);
    assertEquals((count + 2) * threads, sum);

    FileUtil.rmr(dir);
  }

  /**
   * Test case that exercises one shared walManager, many ackedwriteahead decos,
   * many threads. THis is different from the previous because it allows for
   * contention on the open calls.
   */
  void doSharedWALManagerOpenContention(final int count, final int threads)
      throws IOException, InterruptedException {
    File dir = FileUtil.mktempdir();
    final CountDownLatch start = new CountDownLatch(threads);
    final CountDownLatch done = new CountDownLatch(threads);
    final NaiveFileWALManager wal = new NaiveFileWALManager(dir);
    wal.open();

    for (int i = 0; i < threads; i++) {
      final int idx = i;
      new Thread() {
        @Override
        public void run() {
          start.countDown();
          try {
            EventSource src = new NoNlASCIISynthSource(count, 100);
            Context ctx = new ReportTestingContext();
            EventSink snk = new CompositeSink(ctx, "counter(\"total." + idx
                + "\")");
            // use the same wal, but different counter.

            snk = new NaiveFileWALDeco(ctx, snk, wal, new TimeTrigger(
                new ProcessTagger(), 1000000), new AckListener.Empty(), 1000000);

            start.await();

            // allow for contention on the open call.
            src.open();
            snk.open();

            EventUtil.dumpAll(src, snk);
            src.close();
            snk.close();
          } catch (Exception e) {
            LOG.error("failure", e);
            // fail("e");
          } finally {
            done.countDown();
          }
        }

      }.start();
    }

    boolean ok = done.await(30, TimeUnit.SECONDS);
    assertTrue("Test timed out!", ok);
    Thread.sleep(1000);

    long sum = 0;
    for (int i = 0; i < threads; i++) {
      CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
          "total." + i);
      long ci = cnt.getCount();
      LOG.info("count " + i + " : " + ci);
      sum += ci;

    }

    LOG.info("sum == " + sum);
    assertEquals((count + 2) * threads, sum);

    FileUtil.rmr(dir);
  }

  /**
   * Shared wal deco, small amount of concurrency
   */
  @Test
  public void testSharedDecoSmall() throws IOException, InterruptedException,
      FlumeSpecException {
    doSharedWALDeco(10000, 10);
  }

  /**
   * Shared wal deco, large amount of concurrency
   */
  @Test
  public void testSharedDecoLarge() throws IOException, InterruptedException,
      FlumeSpecException {
    doSharedWALDeco(1000, 100);
  }

  /**
   * Shared wal deco, huge amount of concurrency
   */
  @Test
  public void testSharedDecoHuge() throws IOException, InterruptedException,
      FlumeSpecException {
    doSharedWALDeco(100, 1000);
  }

  // TODO (jon) currently multiple wals decos usig the same walManager has
  // problems.

  /**
   * Shared wal man, independent wal decos, small amount of concurrency
   */
  @Ignore
  @Test
  public void testSharedWALManSmall() throws IOException, InterruptedException {
    doSharedWALManager(10000, 10);
  }

  /**
   * Shared wal man, independent wal decos, large amount of concurrency
   */
  @Ignore
  @Test
  public void testSharedWALManLarge() throws IOException, InterruptedException {
    doSharedWALManager(1000, 100);
  }

  /*
   * Shared wal man, independent wal decos, huge amount of concurrency
   */
  @Ignore
  @Test
  public void testSharedWALManHuge() throws IOException, InterruptedException {
    doSharedWALManager(100, 1000);
  }

  /**
   * Shared wal man, independent wal decos, small amount of concurrency,
   * contention on open calls
   */
  @Ignore
  @Test
  public void testSharedWALOpenContendManSmall() throws IOException,
      InterruptedException {
    doSharedWALManager(10000, 10);
  }

  /**
   * Shared wal man, independent wal decos, large amount of concurrency,
   * contention on open calls
   */
  @Ignore
  @Test
  public void testSharedWALManOpenContendLarge() throws IOException,
      InterruptedException {
    doSharedWALManager(1000, 100);
  }

  /**
   * Shared wal man, independent wal decos, huge amount of concurrency,
   * contention on open calls
   */
  @Ignore
  @Test
  public void testSharedWALManOpenContendHuge() throws IOException,
      InterruptedException {
    doSharedWALManager(100, 1000);
  }

}
