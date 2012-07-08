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
    LOG.info("============================================================");
  }

  /**
   * Test case exercises one wal manager, one ackedwriteahead deco, many
   * threads.
   */
  public void doSharedWALDeco(final int count, final int threads)
      throws IOException, InterruptedException, FlumeSpecException {

    File dir = FileUtil.mktempdir();
    try {
      final CountDownLatch start = new CountDownLatch(threads);
      final CountDownLatch done = new CountDownLatch(threads);
      final NaiveFileWALManager wal = new NaiveFileWALManager(dir);
      wal.open();

      Context ctx = new ReportTestingContext();
      EventSink cntsnk = new CompositeSink(ctx, "counter(\"total\")");
      // use the same wal, but different counter.
      final EventSink snk = new NaiveFileWALDeco(ctx, cntsnk, wal,
          new TimeTrigger(new ProcessTagger(), 1000000),
          new AckListener.Empty(), 1000000);
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

      CounterSink cnt = (CounterSink) ReportManager.get()
          .getReportable("total");
      long ci = cnt.getCount();
      LOG.info("count : " + ci);
      assertEquals((count * threads) + 2, ci);
    } finally {
      FileUtil.rmr(dir);
    }
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

}
