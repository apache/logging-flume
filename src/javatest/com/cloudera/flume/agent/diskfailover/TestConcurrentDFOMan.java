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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.agent.DirectMasterRPC;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.FileUtil;

/**
 * This tests concurrent wal managers writing to two different directories. Not
 * the best solution but workable.
 */
public class TestConcurrentDFOMan {
  public static Logger LOG = Logger.getLogger(TestConcurrentDFOMan.class);

  @Before
  public void setDebug() {
    Logger.getLogger(CounterSink.class).setLevel(Level.DEBUG);
    Logger.getLogger(AccumulatorSink.class).setLevel(Level.DEBUG);
    Logger.getLogger(RollSink.class).setLevel(Level.DEBUG);
  }

  @Test
  public void test1thread() throws IOException, InterruptedException {
    doTestConcurrentDFOMans(1, 10000, 60000);
  }

  @Test
  public void test10thread() throws IOException, InterruptedException {
    doTestConcurrentDFOMans(10, 10000, 60000);
  }

  @Test
  public void test100thread() throws IOException, InterruptedException {
    doTestConcurrentDFOMans(100, 1000, 60000);
  }

  @Test
  @Ignore("Test takes too long")
  public void test1000thread() throws IOException, InterruptedException {
    doTestConcurrentDFOMans(1000, 100, 120000);
  }

  @Test
  public void test5logicalNodesHuge() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(5, 10000, 180000);
  }

  @Test
  public void test10logicalNodesHuge() throws IOException,
      InterruptedException, FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(10, 10000, 180000);
  }

  @Test
  public void test10logicalNodesSmall() throws IOException,
      InterruptedException, FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(10, 1000, 60000);
  }

  @Test
  public void test10logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(10, 10000, 120000);
  }

  @Test
  public void test100logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(100, 1000, 60000);
  }

  @Test
  @Ignore("takes too long")
  public void test100logicalNodesBig() throws IOException,
      InterruptedException, FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(100, 10000, 1800000);
  }

  @Test
  @Ignore("takes too long")
  public void test1000logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestLogicalNodesConcurrentDFOMans(1000, 100, 60000);
  }

  /**
   * Bang on the wal mechanism as hard was you want with number of concurrent
   * threads, number of events per thread. Timeout in millis,
   */
  public void doTestConcurrentDFOMans(final int threads, final int events,
      int timeout) throws IOException, InterruptedException {

    final CountDownLatch started = new CountDownLatch(threads);
    final CountDownLatch done = new CountDownLatch(threads);
    final DiskFailoverManager[] dfos = new DiskFailoverManager[threads];

    for (int i = 0; i < threads; i++) {
      final int idx = i;
      new Thread("Concurrent-" + i) {
        @Override
        public void run() {
          try {
            File f1 = FileUtil.mktempdir();
            AccumulatorSink cnt1 = new AccumulatorSink("count." + idx);
            DiskFailoverManager dfoMan = new NaiveFileFailoverManager(f1);
            dfos[idx] = dfoMan; // save for checking.

            // short trigger causes lots of rolls
            EventSink snk = new DiskFailoverDeco<EventSink>(cnt1, dfoMan,
                new TimeTrigger(100), 50);

            ReportManager.get().add(cnt1);
            // make each parallel instance send a slightly different number of
            // messages.
            EventSource src = new NoNlASCIISynthSource(events + idx, 100);

            src.open();
            snk.open();

            started.countDown();

            EventUtil.dumpAll(src, snk);
            src.close();
            snk.close(); // this triggers a flush of current file!?
            FileUtil.rmr(f1);
          } catch (Exception e) {
            LOG.error(e, e);
          } finally {
            done.countDown();
          }

        }
      }.start();

    }

    started.await();
    boolean ok = done.await(timeout, TimeUnit.MILLISECONDS);
    assertTrue("Test timed out", ok);

    for (int i = 0; i < threads; i++) {
      AccumulatorSink cnt = (AccumulatorSink) ReportManager.get()
          .getReportable("count." + i);
      // check for the slightly different counts based on thread.
      int exp = events + i;
      LOG
          .info("count." + i + " expected " + exp + " and got "
              + cnt.getCount());
      assertEquals(exp, (int) cnt.getCount());

      // check dfo reports to see if they are sane.
      ReportEvent rpt = dfos[i].getReport();
      LOG.info(rpt);
      long failovered = rpt.getLongMetric(DiskFailoverManager.A_MSG_WRITING);
      assertEquals(events + i, failovered);
    }
  }

  public void doTestLogicalNodesConcurrentDFOMans(final int threads,
      final int events, int timeout) throws IOException, InterruptedException,
      FlumeSpecException {
    BenchmarkHarness.setupLocalWriteDir();
    FlumeMaster master = new FlumeMaster();
    FlumeNode node = new FlumeNode(new DirectMasterRPC(master), false, false);
    final Reportable[] dfos = new Reportable[threads];

    for (int i = 0; i < threads; i++) {
      String name = "test." + i;
      String report = "report." + i;
      int count = events + i;
      String src = "asciisynth(" + count + ",100)";
      String snk = "{ diskFailover => counter(\"" + report + "\") } ";
      node.getLogicalNodeManager().testingSpawn(name, src, snk);
      dfos[i] = node.getLogicalNodeManager().get(name);
    }

    // TODO (jon) using sleep is cheating to give all threads a chance to start.
    // Test seems flakey without this due to a race condition.
    Thread.sleep(500);

    // wait for all to be done.
    waitForEmptyDFOs(node, timeout);

    // check to make sure everyone got the right number of events
    boolean success = true;

    for (int i = 0; i < threads; i++) {
      LOG.info(dfos[i].getReport());
    }

    for (int i = 0; i < threads; i++) {

      CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
          "report." + i);
      LOG.info(i + " expected " + (events + i) + " and got " + cnt.getCount());
      success &= ((events + i) == cnt.getCount());
      assertEquals(events + i, cnt.getCount());

    }
    assertTrue("Counts did not line up", success);

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * Wait until the flume node's dfos are empty.
   */
  private void waitForEmptyDFOs(FlumeNode node, int timeout)
      throws InterruptedException {
    boolean done = false;
    long start = System.currentTimeMillis();
    while (!done) {
      if (System.currentTimeMillis() - start > timeout) {
        fail("Test took too long");
      }
      Collection<LogicalNode> lns = node.getLogicalNodeManager().getNodes();
      done = areDFOsReconfigured(lns) && areDFOsEmpty(lns);
      if (!done) {
        Thread.sleep(250);
      }
    }
  }

  boolean areDFOsReconfigured(Collection<LogicalNode> lns) {
    for (LogicalNode n : lns) {
      long val = n.getReport().getLongMetric(LogicalNode.A_RECONFIGURES);
      if (val == 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the DFOs associated with logical node list are currently empty.
   */
  boolean areDFOsEmpty(Collection<LogicalNode> lns) {
    for (LogicalNode n : lns) {
      DiskFailoverManager dfo = FlumeNode.getInstance().getDFOManager(
          n.getName());

      if (!dfo.isEmpty())
        return false;
    }
    return true;
  }
}
