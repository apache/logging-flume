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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.diskfailover.DiskFailoverDeco;
import com.cloudera.flume.agent.diskfailover.DiskFailoverManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeArgException;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.BackOffFailOverSink;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;

public class TestAgentFailChainSink {
  static final Logger LOG = LoggerFactory
      .getLogger(TestAgentFailChainSink.class);

  /**
   * These should fail if there are any exceptions thrown.
   */
  @Test
  public void testWALFailchain() throws FlumeSpecException, JSONException {
    ReportManager.get().clear();
    String spec = AgentFailChainSink.genE2EChain("counter(\"foo1\")",
        "counter(\"foo2\")", "counter(\"foo3\")");

    LOG.info("waled failchain: " + spec);
    String node = NetUtils.localhost();
    EventSink snk = FlumeBuilder.buildSink(new LogicalNodeContext(node, node),
        spec);
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());

    // just check a sample of the values:
    assertEquals(0, (long) rpt.getLongMetric("naiveWal.loggedQ"));
    assertEquals("foo1",
        rpt.getStringMetric("drainSink.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.primary.foo1.name"));
    assertEquals("foo2",
        rpt.getStringMetric("drainSink.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.backup.BackoffFailover.primary.foo2.name"));
    assertEquals("foo3",
        rpt.getStringMetric("drainSink.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.backup.BackoffFailover.backup.foo3.name"));
  }

  @Test
  public void testWALChain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentE2EChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    String node = NetUtils.localhost();
    new CompositeSink(new LogicalNodeContext(node, node), spec);
  }

  @Test(expected = FlumeArgException.class)
  public void testWALChainBadContext() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentE2EChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    new CompositeSink(new Context(), spec);
  }

  /**
   * These should fail if there are any exceptions thrown.
   */
  @Test
  public void testBEFailchain() throws FlumeSpecException, JSONException {
    ReportManager.get().clear();
    String spec = AgentFailChainSink.genBestEffortChain("counter(\"foo1\")",
        "counter(\"foo2\")", "counter(\"foo3\")");
    LOG.info("best effort failchain: " + spec);
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());

    // just check a sample of the values:
    assertEquals(
        "foo1",
        rpt.getStringMetric("primary.LazyOpenDecorator.StubbornAppend.foo1.name"));
    assertEquals("foo2",
        rpt.getStringMetric("backup.BackoffFailover.primary.LazyOpenDecorator."
            + "StubbornAppend.foo2.name"));
    assertEquals("foo3",
        rpt.getStringMetric("backup.BackoffFailover.backup.LazyOpenDecorator."
            + "StubbornAppend.foo3.name"));
  }

  @Test
  public void testBEChain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentBEChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    new CompositeSink(new Context(), spec);
  }

  /**
   * have two destinations, send some messsage to primary, kill primary, send
   * some to secondary, kill secondary, send some messages to null, restore
   * secondary send some messagse to secondary.
   */
  @Test
  public void testConfirmBEChain() throws FlumeSpecException, IOException,
      InterruptedException {
    // create sources
    String c1 = "rpcSource(1234)";
    ThriftEventSource c1Src = (ThriftEventSource) FlumeBuilder.buildSource(
        LogicalNodeContext.testingContext(), c1);
    c1Src.open();

    String c2 = "rpcSource(1235)";
    ThriftEventSource c2Src = (ThriftEventSource) FlumeBuilder.buildSource(
        LogicalNodeContext.testingContext(), c2);
    c2Src.open();

    // create agentBEChain sink
    String spec = "agentBEChain(\"localhost:1234\", \"localhost:1235\")";
    EventSink snk = new CompositeSink(new Context(), spec);
    snk.open();

    Event e1 = new EventImpl("test 1".getBytes());
    Event e2 = new EventImpl("test 2".getBytes());
    Event e3 = new EventImpl("test 3".getBytes());
    Event e4 = new EventImpl("test 4".getBytes());

    // Everything is on and we send some messages
    snk.append(e1);
    Clock.sleep(100);
    LOG.info(c1Src.getMetrics().toString());
    assertEquals(1,
        (long) c1Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    c1Src.next();
    c1Src.close();

    // Killed the first of the chain, should go to backup
    // the number of events lost here is not consistent after close. this
    // seems time based, and the first two seem to be lost
    snk.append(e1);
    Clock.sleep(20);
    snk.append(e2);
    Clock.sleep(20);
    snk.append(e3);
    Clock.sleep(20);
    snk.append(e4);
    Clock.sleep(20);

    LOG.info(c2Src.getMetrics().toString());
    assertEquals(2,
        (long) c2Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    // 2 lost in network buffer, but two received in backup.  yay.
    c2Src.next();
    c2Src.next();
    c2Src.close();

    // all thrift sinks are closed now, we should loss messages
    snk.append(e1); // lost
    Clock.sleep(20);
    snk.append(e2); // lost
    Clock.sleep(20);
    snk.append(e3); // lost
    Clock.sleep(20);
    snk.append(e4); // lost
    Clock.sleep(20);

    // re-open desination 1.
    c1Src.open();
    snk.append(e1);
    Clock.sleep(20);
    snk.append(e2);
    Clock.sleep(20);
    c1Src.next();
    c1Src.close();
    LOG.info(c1Src.getMetrics().toString());
    // 2 events from prevoius + 1 from new open
    // first one fails on reopen but next succeeds
    assertEquals(2 + 1,
        (long) c2Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    snk.close();
  }

  /**
   * These should fail if there are any exceptions thrown.
   */
  @Test
  public void testDFOFailchain() throws FlumeSpecException, JSONException {
    ReportManager.get().clear();
    String spec = AgentFailChainSink.genDfoChain("counter(\"foo1\")",
        "counter(\"foo2\")", "counter(\"foo3\")");
    LOG.info("disk failover failchain: " + spec);
    String node = NetUtils.localhost();
    EventSink snk = FlumeBuilder.buildSink(new LogicalNodeContext(node, node),
        spec);
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());

    // just check a sample of the values:
    assertEquals(
        "foo1",
        rpt.getStringMetric("backup.DiskFailover.LazyOpenDecorator."
            + "InsistentAppend.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.primary.LazyOpenDecorator.StubbornAppend.foo1.name"));

    assertEquals(
        "foo2",
        rpt.getStringMetric("backup.DiskFailover.LazyOpenDecorator."
            + "InsistentAppend.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.backup.BackoffFailover.primary.LazyOpenDecorator."
            + "StubbornAppend.foo2.name"));
    assertEquals(
        "foo3",
        rpt.getStringMetric("backup.DiskFailover.LazyOpenDecorator."
            + "InsistentAppend.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.backup.BackoffFailover.backup.LazyOpenDecorator."
            + "StubbornAppend.foo3.name"));
  }

  @Test
  public void testDFOChain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentDFOChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    String node = NetUtils.localhost();
    new CompositeSink(new LogicalNodeContext(node, node), spec);
  }

  @Test(expected = FlumeArgException.class)
  public void testDFOChainBadContext() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentDFOChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    new CompositeSink(new Context(), spec);
  }

  /**
   * have two destinations, send some messsage to primary, kill primary, send
   * some to secondary, kill secondary, send some messages to dfo log, restore
   * secondary send some messages to secondary.
   * 
   * Recover some of secondary.
   */
  @Test
  public void testConfirmDFOChain() throws FlumeSpecException, IOException,
      InterruptedException {
    // create sources
    String c1 = "rpcSource(1234)";
    ThriftEventSource c1Src = (ThriftEventSource) FlumeBuilder.buildSource(
        LogicalNodeContext.testingContext()

        , c1);
    c1Src.open();

    String c2 = "rpcSource(1235)";
    ThriftEventSource c2Src = (ThriftEventSource) FlumeBuilder.buildSource(
        LogicalNodeContext.testingContext(), c2);
    c2Src.open();

    // create agentDFOChain sink
    File tmpDir = FileUtil.mktempdir();
    String spec = "agentDFOChain(\"localhost:1234\", \"localhost:1235\")";
    CompositeSink snk = new CompositeSink(new LogicalNodeContext(
        tmpDir.getName(), tmpDir.getName()), spec);
    snk.open();

    Event e1 = new EventImpl("test 1".getBytes());
    Event e2 = new EventImpl("test 2".getBytes());
    Event e3 = new EventImpl("test 3".getBytes());
    Event e4 = new EventImpl("test 4".getBytes());

    // Everything is on and we send some messages.
    snk.append(e1);
    Clock.sleep(100);
    LOG.info(c1Src.getMetrics().toString());
    assertEquals(1,
        (long) c1Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    // it got through, yay.
    c1Src.next();
    c1Src.close();

    // Killed the first of the chain, should go to backup
    // the number of events lost here is not consistent after close. this
    // seems time based, and the first two seem to be lost
    snk.append(e1);
    Clock.sleep(20);
    snk.append(e2);
    Clock.sleep(20);
    snk.append(e3);
    Clock.sleep(20);
    snk.append(e4);
    Clock.sleep(20);
       
    LOG.info(c2Src.getMetrics().toString());
    assertEquals(2,
        (long) c2Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    // 2 lost in network buffer, but two received in backup. yay.
    c2Src.next();
    c2Src.next();
    c2Src.close();

    // all thrift sinks are closed now, we should end up in dfo
    snk.append(e1); // lost in thrift sink buffer
    Clock.sleep(20);
    snk.append(e2); // lost in thrift sink buffer
    Clock.sleep(20);
    snk.append(e3); // written
    Clock.sleep(20);
    snk.append(e4); // written
    Clock.sleep(20);
    LOG.info(snk.getMetrics().toText());
    assertEquals(
        2,
        (long) ReportUtil.getFlattenedReport(snk).getLongMetric(
            "backup.DiskFailover.NaiveDiskFailover.writingEvts"));

    // re-open destination 1
    c1Src.open();
    snk.append(e1);
    Clock.sleep(20);
    snk.append(e2);
    Clock.sleep(20);
    c1Src.next();

    // get handle to roller in dfo log roller to provide data
    AgentFailChainSink afcs = (AgentFailChainSink) snk.getSink();
    BackOffFailOverSink bofos = (BackOffFailOverSink) ((CompositeSink) afcs.snk)
        .getSink();
    DiskFailoverDeco dfo = (DiskFailoverDeco) bofos.getBackup();
    DiskFailoverManager dfm = dfo.getFailoverManager();
    RollSink dfoWriter = dfo.getDFOWriter();
    dfoWriter.rotate(); // allow dfo retry thread to go.

    // give data some time to show up.
    Clock.sleep(1000);
    c1Src.next();
    c1Src.next();
    c1Src.next();
    c1Src.next();
    c1Src.close();
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(rpt.toString());

    String written = "backup.DiskFailover.NaiveDiskFailover.writingEvts";
    assertEquals(4, (long) rpt.getLongMetric(written));
    // yay. all four events written to dfo log

    String primary = "backup.DiskFailover."
        + "LazyOpenDecorator.InsistentAppend.StubbornAppend."
        + "InsistentOpen.FailoverChainSink.sentPrimary";
    assertEquals(4, (long) rpt.getLongMetric(primary));
    // yay all four go through to the path we wanted. (the primary after the
    // disk failover)

    // data from DFO log was sent.
    assertEquals(2 + 4,
        (long) c1Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));
    // first one fails on reopen but next succeeds
    assertEquals(2 + 1,
        (long) c2Src.getMetrics().getLongMetric(ThriftEventSource.A_ENQUEUED));

  }
}
