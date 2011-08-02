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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.Clock;

/**
 * This tests the disk failover mode's behavior to make sure it works properly.
 */
public class TestDiskFailoverBehavior {

  final public static Logger LOG = Logger
      .getLogger(TestDiskFailoverBehavior.class);

  @Before
  public void setup() {
    BenchmarkHarness.setupLocalWriteDir();
  }

  @After
  public void teardown() throws IOException {
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  LogicalNode setupAgent(long count, String agentSink) throws IOException,
      RuntimeException, FlumeSpecException {
    LogicalNode agent = new LogicalNode(
        new LogicalNodeContext("phys", "agent"), "agent");
    FlumeConfigData fcd = new FlumeConfigData(0, "asciisynth(" + count + ")",
        agentSink, 1, 1, "flow");
    agent.loadConfig(fcd);
    return agent;
  }

  LogicalNode setupColl(long port, String name, String acc) throws IOException,
      RuntimeException, FlumeSpecException {
    Context ctx = new LogicalNodeContext(new ReportTestingContext(), "phys",
        name);
    LogicalNode coll = new LogicalNode(ctx, name);
    FlumeConfigData fcd2 = new FlumeConfigData(0, "rpcSource(" + port + ")",
        "accumulator(\"" + acc + "\")", 1, 1, "flow");
    coll.loadConfig(fcd2);
    return coll;
  }

  void loopUntilCount(long count, LogicalNode coll, LogicalNode coll2)
      throws InterruptedException {
    boolean done = false;
    int loops = 0;
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    while (!done) {
      Clock.sleep(1000);
      long cnt1 = (ctr == null) ? 0 : ctr.getCount();
      long cnt2 = (ctr2 == null) ? 0 : ctr2.getCount();

      LOG.info("loop " + loops + " collector count = " + cnt1 + " count2 = "
          + cnt2);
      if (coll != null) {
        LOG.info(coll.getReport().toText());
      }
      LOG.info(coll2.getReport().toText());

      if (cnt1 + cnt2 >= count)
        break;
      loops++;
    }

  }

  /**
   * Test the DFO failure path assuming the retry sink is reliable, e.g. the
   * secondary/failover in this case will not error out.
   */
  @Test
  public void testDFOPerfectRetry() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    long count = 1000;

    // start the collectors first
    LogicalNode coll = setupColl(12345, "coll", "count");
    LogicalNode coll2 = setupColl(12346, "coll2", "count2");

    // then the agent so it can connect
    String agentSink = "< { flakeyAppend(.1, 1337) => rpcSink(\"localhost\",12345) } ?"
        + " {diskFailover => {insistentAppend => { lazyOpen "
        + "=> rpcSink(\"localhost\",12345) } } } >";
    LogicalNode agent = setupAgent(count, agentSink);

    // wait until the counts add up properly
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    loopUntilCount(count, coll, coll2);

    assertEquals(NodeState.IDLE, agent.getStatus().state);

    // close off the collector
    coll.close();
    coll2.close();
    agent.close();

    // check outout
    LOG.info("primary collector count   = " + ctr.getCount());
    LOG.info("secondary collector count = " + ctr2.getCount());
    assertEquals(count, ctr.getCount() + ctr2.getCount());
    assertEquals(NodeState.IDLE, coll.getStatus().state);
    assertEquals(NodeState.IDLE, coll2.getStatus().state);
  }

  /**
   * Same test, now with a flakey sink to the diskFailover. This case is
   * supposed to propagate the problem through to the parent and throw some sort
   * of exception.
   */
  @Test
  public void testDFOFlakeyRetry() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    long count = 1000;

    // start the collectors first
    LogicalNode coll = setupColl(12345, "coll", "count");
    LogicalNode coll2 = setupColl(12346, "coll2", "count2");

    // Then the agent so it can connect. This version assumes that the
    // secondary/failover case will fail and pass
    // an exception back the primary.
    String agentSink = "< { flakeyAppend(.1,1337) => rpcSink(\"localhost\",12345) } ?"
        + " {diskFailover => {lazyOpen => {flakeyAppend(.1,1337) "
        + "=> rpcSink(\"localhost\",12346) } } } >";
    LogicalNode agent = setupAgent(count, agentSink);

    // wait for agent done
    // wait until the counts add up properly
    boolean done = false;
    int loops = 0;
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    long old = 0;
    while (!done) {
      Clock.sleep(1000);

      LOG.info("loop " + loops + " collector count = " + ctr.getCount()
          + " count2 = " + ctr2.getCount());

      LOG.info(coll.getReport().toText());
      LOG.info(coll2.getReport().toText());
      if (old == ctr.getCount()) {
        break;
      }
      old = ctr.getCount();
      loops++;
    }

    // close off the collector
    coll.close();
    coll2.close();
    agent.close();

  }

  /**
   * Test the DFO failure path with an unreliable subsink that has been made
   * decorated so that it will not give up trying to send. in this case, despite
   * the flakey rpc, the inisitentAppend and stubbornAppend make the sink
   * resilient, and never give up.
   */
  @Test
  public void testDFOInsistentRetry() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    long count = 100;

    // start the collectors first
    LogicalNode coll = setupColl(12345, "coll", "count");
    LogicalNode coll2 = setupColl(12346, "coll2", "count2");

    // Then the agent so it can connect. This config will attempt to send on the
    // primary, and when if fails goes to writing to disk. The subsink of
    // diskFailover is decorated to never return with an exception.
    String agentSink = "{ delay(100) => < "
        + "{ flakeyAppend(.05) => rpcSink(\"localhost\",12345) } ?"
        + " {diskFailover => { insistentAppend => { stubbornAppend => { insistentOpen "
        + "=> { lazyOpen => {flakeyAppend(.05) => rpcSink(\"localhost\",12346) } } }  } } }> } ";
    LogicalNode agent = setupAgent(count, agentSink);

    // wait until the counts add up properly
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    loopUntilCount(count, coll, coll2);

    // close off the collector
    coll.close();
    coll2.close();

    // dump info for debugging
    Map<String, ReportEvent> rpts = new HashMap<String, ReportEvent>();
    agent.getReports(rpts);
    for (Entry<String, ReportEvent> e : rpts.entrySet()) {
      LOG.info(e.getKey() + " : " + e.getValue());
    }

    // check the end states
    assertEquals(count, ctr.getCount() + ctr2.getCount());
    assertEquals(NodeState.IDLE, coll2.getStatus().state);
    assertEquals(NodeState.IDLE, coll.getStatus().state);
  }

  /**
   * Test the DFO cases where collectors are not initially up, but show up later
   */
  @Test
  public void testDFOCollectorsNotUp() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    long count = 100;

    // Start the agent first.
    String agentSink = "{ delay(100) => < "
        + "{ flakeyAppend(.05) => rpcSink(\"localhost\",12345) } ?"
        + " {diskFailover => { insistentAppend => { stubbornAppend => { insistentOpen "
        + "=> { lazyOpen => {flakeyAppend(.05) => rpcSink(\"localhost\",12346) } } }  } } }> } ";
    LogicalNode agent = setupAgent(count, agentSink);

    // Purposely sleep a little so that the agent is collecting to disk, then
    // start collectors
    Clock.sleep(2000);
    LogicalNode coll = setupColl(12345, "coll", "count");
    LogicalNode coll2 = setupColl(12346, "coll2", "count2");

    // wait until the counts add up properly
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    loopUntilCount(count, coll, coll2);

    // close off the collector
    coll.close();
    coll2.close();

    // dump info for debugging
    Map<String, ReportEvent> rpts = new HashMap<String, ReportEvent>();
    agent.getReports(rpts);
    for (Entry<String, ReportEvent> e : rpts.entrySet()) {
      LOG.info(e.getKey() + " : " + e.getValue());
    }

    // check the end states
    assertEquals(count, ctr.getCount() + ctr2.getCount());
    assertTrue(ctr.getCount() > 0);
    assertTrue(ctr2.getCount() > 0);
    assertEquals(NodeState.IDLE, coll2.getStatus().state);
    assertEquals(NodeState.IDLE, coll.getStatus().state);
  }

  /**
   * This tests the DFO case where the primary is never even started.
   */
  @Test
  public void testDFOCollectors1NotUp() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    long count = 100;

    // Start the agent first.
    String agentSink = "{ delay(100) => < "
        + "{ flakeyAppend(.05) => rpcSink(\"localhost\",12345) } ?"
        + " {diskFailover => { insistentAppend => { stubbornAppend => { insistentOpen "
        + "=> { lazyOpen => {flakeyAppend(.05) => rpcSink(\"localhost\",12346) } } }  } } }> } ";
    LogicalNode agent = setupAgent(count, agentSink);

    // Purposely sleep a little so that the agent is collecting to disk, then
    // start collectors
    Clock.sleep(2000);
    // LogicalNode coll = setupColl(12345, "coll", "count");
    LogicalNode coll2 = setupColl(12346, "coll2", "count2");

    // wait until the counts add up properly
    AccumulatorSink ctr2 = (AccumulatorSink) ReportManager.get().getReportable(
        "count2");
    loopUntilCount(count, null, coll2);

    // close off the collector
    // coll.close();
    coll2.close();

    // dump info for debugging
    Map<String, ReportEvent> rpts = new HashMap<String, ReportEvent>();
    agent.getReports(rpts);
    for (Entry<String, ReportEvent> e : rpts.entrySet()) {
      LOG.info(e.getKey() + " : " + e.getValue());
    }

    // check the end states
    assertEquals(count, ctr2.getCount());
    assertEquals(NodeState.IDLE, coll2.getStatus().state);

  }
}
