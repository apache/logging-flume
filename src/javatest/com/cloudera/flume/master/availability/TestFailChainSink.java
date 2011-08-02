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
package com.cloudera.flume.master.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.util.AlwaysRetryPolicy;

/**
 * Test the failover chain sink.
 */
public class TestFailChainSink {

  final static Logger LOG = Logger.getLogger(TestFailChainSink.class);

  @Before
  public void debugSettings() {
    Logger.getLogger(FailoverChainSink.class).setLevel(Level.DEBUG);
    Logger.getLogger(AccumulatorSink.class).setLevel(Level.ERROR);
  }

  /**
   * this simulates having 5 failovers, but they write to 5 accumulators
   * instead. We We use the simplistic failover mechanism
   * 
   * @throws InterruptedException
   */
  @Test
  public void testAvailableSinkGen() throws IOException, FlumeSpecException,
      InterruptedException {

    List<String> names = Arrays.asList("first", "second", "third", "fourth",
        "fifth");
    FailoverChainSink snk = new FailoverChainSink(new ReportTestingContext(),
        "{ lazyOpen => { intervalFlakeyAppend(2) => accumulator(\"%s\")}}",
        names, new AlwaysRetryPolicy());

    // failover sink replaces each names

    LOG.info(snk.getMetrics().toText());

    snk.open();
    EventSource src = MemorySinkSource.cannedData("test is a test", 31);
    src.open();
    EventUtil.dumpAll(src, snk);

    int[] ans = { 16, 8, 4, 2, 1 };
    for (int i = 0; i < ans.length; i++) {
      Reportable rptable = ReportManager.get().getReportable(names.get(i));
      long val = rptable.getMetrics().getLongMetric(names.get(i));
      assertEquals(ans[i], val);
    }

    src.open();
    try {
      // here we finally have all failovers triggered to fail
      snk.append(src.next());
    } catch (IOException ioe) {
      // this should be thrown and caught.
      src.close();
      snk.close();
      return;
    }

    fail("Expected exception");
  }

  /**
   * this simulates having 5 failovers, but they write to 5 counters instead. We
   * use the AvailabilityManager's spec generator -- to write an availableSink
   * specification that would acutally get shipped to the agent.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testAvailableSinkBuilder() throws IOException,
      FlumeSpecException, InterruptedException {
    // this is equivalent of NeverBackoff
    FlumeConfiguration.get().setInt(
        FlumeConfiguration.AGENT_FAILOVER_INITIAL_BACKOFF, 0);

    List<String> names = Arrays.asList("first", "second", "third", "fourth",
        "fifth");
    String body = "{ lazyOpen => { intervalFlakeyAppend(2) => accumulator(\"%s\")}}";
    String spec = FailoverChainManager.genAvailableSinkSpec(body, names);
    System.out.println(spec);
    EventSink snk = new CompositeSink(new ReportTestingContext(), spec);

    LOG.info(snk.getMetrics().toText());

    snk.open();
    EventSource src = MemorySinkSource.cannedData("test is a test", 31);
    src.open();
    EventUtil.dumpAll(src, snk);

    int[] ans = { 16, 8, 4, 2, 1 };
    for (int i = 0; i < ans.length; i++) {
      Reportable rptable = ReportManager.get().getReportable(names.get(i));
      long val = rptable.getMetrics().getLongMetric(names.get(i));
      System.out.println("report " + names.get(i) + " : " + val);
      System.out.flush();
      assertEquals(ans[i], val);
    }
  }

  /**
   * The next tests verify that the macro sinks can be generated, and will throw
   * exceptions when opened unless they are shadowed out by let statements.
   * 
   * @throws InterruptedException
   */
  @Test(expected = IOException.class)
  public void testAutoBEChain() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBuilder.buildSink(new Context(), "autoBEChain").open();
  }

  @Test(expected = IOException.class)
  public void testAutoDFOChain() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBuilder.buildSink(new Context(), "autoDFOChain").open();
  }

  @Test(expected = IOException.class)
  public void testAutoE2EChain() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBuilder.buildSink(new Context(), "autoE2EChain").open();
  }

}
