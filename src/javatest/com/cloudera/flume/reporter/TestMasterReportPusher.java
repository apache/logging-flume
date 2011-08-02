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
package com.cloudera.flume.reporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cloudera.flume.agent.MockMasterRPC;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;

/**
 * Test cases for the MasterReportPusher, which deposits ReportEvents on the
 * master.
 */
public class TestMasterReportPusher {

  final protected CountDownLatch latch = new CountDownLatch(1);

  /**
   * Specialise MockMasterRPC to allow us to tell when putReports has been
   * called.
   */
  public class PusherMockMaster extends MockMasterRPC {

    public Map<String, ReportEvent> reports;

    @Override
    public void putReports(Map<String, ReportEvent> reports) throws IOException {
      this.reports = reports;
      latch.countDown();
    }
  }

  /**
   * Test full pusher lifecycle - make sure that reports get sent to the master.
   */
  @Test
  public void testPusherThread() throws InterruptedException {
    ReportManager rptMan = ReportManager.get();
    rptMan.clear();

    PusherMockMaster master = new PusherMockMaster();

    MasterReportPusher reportPusher = new MasterReportPusher(FlumeConfiguration
        .createTestableConfiguration(), ReportManager.get(), master);

    MockClock mockClock = new MockClock(0);
    Clock.setClock(mockClock);

    Reportable reportable = new Reportable() {

      @Override
      public String getName() {
        return "test-reportable";
      }

      @Override
      public ReportEvent getMetrics() {
        ReportEvent r = new ReportEvent("test-reportable-report");
        r.setStringMetric("foo", "bar");
        return r;
      }

      @Override
      public Map<String, Reportable> getSubMetrics() {
        return ReportUtil.noChildren();
      }
    };

    rptMan.add(reportable);

    reportPusher.start();

    // Trigger the report pusher
    mockClock.forward(FlumeConfiguration.get().getReporterPollPeriod() * 2);

    // Wait for 'real' 5s delay for report pusher to a) get on a CPU and b) do
    // the push
    assertTrue("putReports not called within 5 seconds", latch.await(5,
        TimeUnit.SECONDS));

    reportPusher.stop();

    assertEquals("Not enough reports received", 1, master.reports.size());
    assertEquals("Report metrics not correctly sent", "bar", master.reports
        .get("prefix.test-reportable").getStringMetric("foo"));

  }
}
