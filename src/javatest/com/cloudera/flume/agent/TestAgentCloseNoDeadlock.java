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
package com.cloudera.flume.agent;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;

/**
 * These test cases verify that closing an agent eventually exits instead of
 * deadlocking.
 * 
 * TODO Currently, the closing of a logical node is not deterministic and can
 * fall into a hard exit path that takes about 30s. This should be improved when
 * the concurrency story gets better defined and a cleaner close/exits can be
 * made.
 */
public class TestAgentCloseNoDeadlock {

  final public static Logger LOG = Logger
      .getLogger(TestAgentCloseNoDeadlock.class);

  public void doReportDeadlockTest(final String sink) throws IOException,
      FlumeSpecException, InterruptedException {
    final CountDownLatch go = new CountDownLatch(1);
    final CountDownLatch heartstop = new CountDownLatch(3);

    final LogicalNodeManager lnm = new LogicalNodeManager("local");

    // simulate the heartbeat
    new Thread("sim heartbeat") {
      @Override
      public void run() {
        try {
          go.await();
          while (heartstop.getCount() > 0) {
            lnm.spawn("foo1", "asciisynth(1)", sink);
            heartstop.countDown();
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          e.printStackTrace();
        }
      }
    }.start();

    // simulate the report pusher
    new Thread("sim report pusher") {
      @Override
      public void run() {
        try {
          go.await();
          while (heartstop.getCount() > 0) {
            lnm.getReport();
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          e.printStackTrace();
        }
      }
    }.start();

    go.countDown();
    assertTrue("heartbeat thread blocked", heartstop.await(200,
        TimeUnit.SECONDS));
  }

  /**
   * This tests verifies that getReport does not cause a deadlock.
   */
  @Test
  public void testNoGetReportDeadLockRpcBE() throws IOException,
      FlumeSpecException, InterruptedException {
    doReportDeadlockTest("agentBESink");
  }

  /**
   * This tests verifies that getReport does not cause a deadlock.
   */
  @Test
  public void testNoGetReportDeadLockDFO() throws IOException,
      FlumeSpecException, InterruptedException {
    doReportDeadlockTest("agentDFOSink");
  }

  /**
   * This tests verifies that getReport does not cause a deadlock.
   */
  @Test
  public void testNoGetReportDeadLockE2E() throws IOException,
      FlumeSpecException, InterruptedException {
    doReportDeadlockTest("agentE2ESink");
  }
}
