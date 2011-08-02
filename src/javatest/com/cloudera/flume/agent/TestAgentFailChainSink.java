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

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeArgException;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportUtil;
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
    assertEquals("foo1", rpt
        .getStringMetric("drainSink.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.primary.foo1.name"));
    assertEquals("foo2", rpt
        .getStringMetric("drainSink.StubbornAppend.InsistentOpen."
            + "FailoverChainSink.backup.BackoffFailover.primary.foo2.name"));
    assertEquals("foo3", rpt
        .getStringMetric("drainSink.StubbornAppend.InsistentOpen."
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
    assertEquals("foo1", rpt
        .getStringMetric("primary.LazyOpenDecorator.StubbornAppend.foo1.name"));
    assertEquals("foo2", rpt
        .getStringMetric("backup.BackoffFailover.primary.LazyOpenDecorator."
            + "StubbornAppend.foo2.name"));
    assertEquals("foo3", rpt
        .getStringMetric("backup.BackoffFailover.backup.LazyOpenDecorator."
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
        rpt
            .getStringMetric("backup.DiskFailover.drainSink.LazyOpenDecorator.InsistentOpen."
                + "FailoverChainSink.primary.LazyOpenDecorator.StubbornAppend.foo1.name"));

    assertEquals(
        "foo2",
        rpt
            .getStringMetric("backup.DiskFailover.drainSink.LazyOpenDecorator.InsistentOpen."
                + "FailoverChainSink.backup.BackoffFailover.primary.LazyOpenDecorator."
                + "StubbornAppend.foo2.name"));
    assertEquals(
        "foo3",
        rpt
            .getStringMetric("backup.DiskFailover.LazyOpenDecorator.InsistentOpen."
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

}
