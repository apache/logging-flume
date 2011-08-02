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
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * This tests concurrent wal managers writing to two different directories. Not
 * the best solution but workable.
 */
public class TestConcurrentWALMan {
  public static Logger LOG = Logger.getLogger(TestConcurrentWALMan.class);

  @Before
  public void setDebug() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Test
  public void test10thread() throws IOException, InterruptedException {
    doTestConcurrentWALMans(10, 10000, 60000);
  }

  @Test
  public void test100thread() throws IOException, InterruptedException {
    doTestConcurrentWALMans(100, 1000, 60000);
  }

  @Ignore("This test times out becuase of inefficient ack retrial/actions")
  @Test
  public void test1000thread() throws IOException, InterruptedException {
    doTestConcurrentWALMans(1000, 100, 120000);
  }

  @Test
  public void test1LogicalNode() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestContextConcurrentWALMans(1, 10000, 120000);
  }

  @Test
  public void test10logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestContextConcurrentWALMans(10, 10000, 120000);
  }

  @Test
  public void test100logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestContextConcurrentWALMans(100, 1000, 120000);
  }

  @Ignore("this test currently blows up")
  @Test
  public void test1000logicalNodes() throws IOException, InterruptedException,
      FlumeSpecException {
    doTestContextConcurrentWALMans(1000, 100, 180000);
  }

  /**
   * Bang on the wal mechanism as hard was you want with number of concurrent
   * threads, number of events per thread. Timeout in millis,
   */
  public void doTestConcurrentWALMans(final int threads, final int events,
      int timeout) throws IOException, InterruptedException {

    final CountDownLatch started = new CountDownLatch(threads);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int i = 0; i < threads; i++) {
      final int idx = i;
      new Thread() {
        @Override
        public void run() {

          try {
            File f1 = FileUtil.mktempdir();

            CounterSink cnt1 = new CounterSink("count." + idx);
            AckChecksumChecker<EventSink> chk = new AckChecksumChecker<EventSink>(
                cnt1);
            NaiveFileWALManager wman1 = new NaiveFileWALManager(f1);
            EventSink snk = new NaiveFileWALDeco<EventSink>(new Context(), chk,
                wman1, new TimeTrigger(1000000), new AckListener.Empty(),
                1000000);

            ReportManager.get().add(cnt1);
            // make each parallel instance send a slightly different number of
            // messages.
            EventSource src = new NoNlASCIISynthSource(events + idx, 100);

            src.open();
            snk.open();

            started.countDown();

            EventUtil.dumpAll(src, snk);
            src.close();
            snk.close();
            FileUtil.rmr(f1);
          } catch (Exception e) {
            LOG.error(e, e);
          } finally {
            done.countDown();
          }

        }
      }.start();

    }

    boolean ok = done.await(timeout, TimeUnit.MILLISECONDS);
    assertTrue("Test timed out", ok);

    for (int i = 0; i < threads; i++) {
      CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
          "count." + i);
      // check for the slightly different counts based on thread.
      int exp = events + i;
      LOG.info("expected " + exp + " but got " + cnt.getCount());
      assertEquals(exp, (int) cnt.getCount());
    }
  }

  boolean isDone(Collection<LogicalNode> lns) {
    for (LogicalNode n : lns) {
      if (!n.getName().startsWith("report")) {
        // skip the default logical node.
        continue;
      }
      if (n.getConfigVersion() == 0 || NodeState.IDLE != n.getStatus().state) {
        return false;
      }
    }
    return true;
  }

  public void doTestContextConcurrentWALMans(final int threads,
      final int events, int timeout) throws IOException, InterruptedException,
      FlumeSpecException {
    BenchmarkHarness.setupLocalWriteDir();
    FlumeMaster master = new FlumeMaster();
    FlumeNode node = new FlumeNode(new DirectMasterRPC(master), false, false);

    for (int i = 0; i < threads; i++) {
      String name = "test." + i;
      String report = "report." + i;
      int count = events + i;
      String src = "asciisynth(" + count + ",100)";
      String snk = " { ackedWriteAhead(15000) => {ackChecker => counter(\""
          + report + "\") }}";

      node.getLogicalNodeManager().testingSpawn(name, src, snk);
    }

    // wait for WALs to flush.
    waitForEmptyWALs(node, timeout);

    // check to make sure everyone got the right number of events
    boolean success = true;
    for (int i = 0; i < threads; i++) {
      CounterSink cnt = (CounterSink) ReportManager.get().getReportable(
          "report." + i);
      LOG.info("expected " + (events + i) + " but got " + cnt.getCount());
      success &= ((events + i) == cnt.getCount());
      assertEquals(events + i, cnt.getCount());
    }
    assertTrue("Counts did not line up", success);
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * Wait until the flume node's WALs are empty.
   */
  private void waitForEmptyWALs(FlumeNode node, int timeout)
      throws InterruptedException {
    boolean done = false;
    long start = System.currentTimeMillis();
    while (!done) {
      if (System.currentTimeMillis() - start > timeout) {
        fail("Test took too long");
      }
      done = areWALsDone(node, node.getLogicalNodeManager().getNodes());
      if (!done) {
        Clock.sleep(2500);
      }
      node.getAckChecker().checkAcks();
    }
  }

  /**
   * Checks if the WALs associated with logical node list are currently empty.
   */
  boolean areWALsDone(FlumeNode node, Collection<LogicalNode> lns) {
    for (LogicalNode n : lns) {
      if (0 >= n.getReport().getLongMetric(LogicalNode.A_RECONFIGURES)) {
        // reconfigure count still at 0
        LOG.warn("Logical node reconfigure count <= 0");
        return false;
      }

      if (n.getStatus().state != NodeState.IDLE) {
        // config not done.
        LOG.warn("Logical node was not IDLE");
        return false;
      }

      if (n.getConfigVersion() <= 0) {
        LOG.warn("Odd, version was 0 but should not be");
        // still on original null/null config
        return false;
      }

      WALManager wal = node.getWalManager(n.getName());

      if (!wal.isEmpty()) {
        LOG.warn(n.getName() + ": wal not empty");
        return false;
      }
    }
    return true;
  }
}
