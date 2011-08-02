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
package com.cloudera.flume.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.util.Pair;
import org.junit.Test;

/**
 * These are essentially the same tests as found in TestFactories, but use the
 * parser and builder infrastructure.
 * 
 * TODO (jon) eventually build code generator so we just test
 * parse/generate/parse.
 */
public class TestFlumeBuilderFunctional implements ExampleData {
  final static Logger LOG = Logger.getLogger(TestFlumeBuilderFunctional.class
      .getName());

  final String SOURCE = "asciisynth(25,100)";
  final static int LINES = 25;

  @Test
  public void testBuildConsole() throws IOException, FlumeSpecException {

    EventSink snk = FlumeBuilder.buildSink(new Context(), "console");
    snk.open();
    snk.append(new EventImpl("test".getBytes()));
    snk.close();
  }

  @Test
  public void testBuildTextSource() throws IOException, FlumeSpecException {
    LOG.info("Working Dir path: " + new File(".").getAbsolutePath());
    EventSource src = FlumeBuilder.buildSource(SOURCE);
    src.open();
    Event e = null;
    int cnt = 0;
    while ((e = src.next()) != null) {
      LOG.info(e);
      cnt++;
    }
    src.close();
    assertEquals(LINES, cnt);
  }

  @Test
  public void testConnector() throws IOException, InterruptedException,
      FlumeSpecException {
    EventSink snk = FlumeBuilder.buildSink(new Context(), "console");
    snk.open();

    EventSource src = FlumeBuilder.buildSource(SOURCE);
    src.open();

    DirectDriver conn = new DirectDriver(src, snk);
    conn.start();

    conn.join(Long.MAX_VALUE);

    snk.close();
    src.close();
    assertTrue(conn.getError() == null);
  }

  @Test
  public void testMultiSink() throws IOException, FlumeSpecException {
    LOG.info("== multi test start");
    String multi = "[ console , accumulator(\"count\") ]";
    EventSource src = FlumeBuilder.buildSource(SOURCE);
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), multi);
    src.open();
    snk.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(LINES, cnt.getCount());
    LOG.info("== multi test stop");
  }

  @Test
  public void testDecorated() throws IOException, FlumeSpecException {
    LOG.info("== Decorated start");
    String decorated = "{ intervalSampler(5) =>  accumulator(\"count\")}";
    // String decorated = "{ intervalSampler(5) =>  console }";

    EventSource src = FlumeBuilder.buildSource(SOURCE);
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        decorated);
    src.open();
    snk.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(LINES / 5, cnt.getCount());
    LOG.info("== Decorated stop");
  }

  @Test
  public void testFailover() throws IOException, FlumeSpecException {
    LOG.info("== failover start");
    // the primary is 90% flakey
    String multi = "< { flakeyAppend(.9,1337) => console } ? accumulator(\"count\") >";
    EventSource src = FlumeBuilder.buildSource(SOURCE);
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), multi);
    src.open();
    snk.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(LINES, cnt.getCount());
    LOG.info("== failover stop");
  }

  /**
   * Functionally tests a let expression. First we use let to create a counter
   * called count. Then we have a failover sink -- the primary is 50% flakey,
   * but goes to count and and the backup is reliable and the same instance of
   * count. 100 messages are sent and the count should count all 100 regardless
   * of the path the message took (primary or backup).
   * 
   * This also checks that open and close is handled correctly. Open should open
   * the let part of the expression, and not attempt to reopen them in the body
   * section.
   * 
   * An accumulator must be used in these tests. Here's why: When a failure is
   * detected on the primary of a failover sink, it periodically attempts to
   * *reopen* the primary sink in order to use it again. The behavior of counter
   * when opened is to reset which is a problem.
   * 
   * How we discovered this: When all tests are run in the same jvm, the backoff
   * timing of the failover gets set to 0 (no backoff) by another test and these
   * tests end up reopening the primary and resetting which restarts the counter
   * at 0. An accumulator just keeps counting.
   */
  @Test
  public void testLet() throws IOException, FlumeSpecException {
    LOG.info("== let and failover start");
    // the primary is 50% flakey but the accumulator should get all the
    // messages.
    String letcount = "let count := accumulator(\"count\") in < { flakeyAppend(.5,1337) => count} ? count >";
    EventSource src = MemorySinkSource.cannedData("canned data ", 100);
    EventSink snk = FlumeBuilder
        .buildSink(new ReportTestingContext(), letcount);
    src.open();
    snk.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();
    AccumulatorSink ctr = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(100, ctr.getCount());
    LOG.info("== let and failover stop");
  }

  /**
   * Lets allow for shadowing, sane semantics dictate the inner scope wins
   */
  @Test
  public void testLetShadow() throws IOException, FlumeSpecException {
    LOG.info("== let shadowing start");
    String let = "let foo := accumulator(\"foo\") in let foo := accumulator(\"bar\") in foo";
    EventSource src = MemorySinkSource.cannedData("canned data ", 100);
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), let);
    src.open();
    snk.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();
    AccumulatorSink fooctr = (AccumulatorSink) ReportManager.get()
        .getReportable("foo");
    AccumulatorSink barctr = (AccumulatorSink) ReportManager.get()
        .getReportable("bar");
    assertEquals(0, fooctr.getCount());
    assertEquals(100, barctr.getCount());
    LOG.info("== let and failover stop");

  }

  @Test
  public void testNode() throws IOException, FlumeSpecException {
    LOG.info("== node start");
    String multi = "localhost : "
        + SOURCE
        + " | < { flakeyAppend(.9,1337) => console } ? accumulator(\"count\") > ;";

    Map<String, Pair<EventSource, EventSink>> cfg = FlumeBuilder.build(
        new ReportTestingContext(), multi);
    for (Entry<String, Pair<EventSource, EventSink>> e : cfg.entrySet()) {
      // String name = e.getKey();
      EventSource src = e.getValue().getLeft();
      EventSink snk = e.getValue().getRight();
      src.open();
      snk.open();
      EventUtil.dumpAll(src, snk);
      src.close();
      snk.close();
    }
    AccumulatorSink cnt = (AccumulatorSink) ReportManager.get().getReportable(
        "count");
    assertEquals(LINES, cnt.getCount());
    LOG.info("== node stop");
  }
}
