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

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportManager;

public class TestAgentFailChainSink {
  static Logger LOG = Logger.getLogger(TestAgentFailChainSink.class);

  /**
   * These should fail if there are any exceptions thrown.
   */
  @Test
  public void testWALFailchain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec =
        AgentFailChainSink.genE2EChain("counter(\"foo1\")",
            "counter(\"foo2\")", "counter(\"foo3\")");

    LOG.info("waled failchain: " + spec);
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    LOG.info(snk.getReport().toJson());
    ReportManager.get().getReportable("foo1");
  }

  @Test
  public void testWALChain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentE2EChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    new CompositeSink(new Context(), spec);
  }

  /**
   * These should fail if there are any exceptions thrown.
   */
  @Test
  public void testBEFailchain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec =
        AgentFailChainSink.genBestEffortChain("counter(\"foo1\")",
            "counter(\"foo2\")", "counter(\"foo3\")");
    LOG.info("best effort failchain: " + spec);
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    LOG.info(snk.getReport().toJson());
    ReportManager.get().getReportable("foo1");
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
  public void testDFOFailchain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec =
        AgentFailChainSink.genDfoChain("counter(\"foo1\")",
            "counter(\"foo2\")", "counter(\"foo3\")");
    LOG.info("disk failover failchain: " + spec);
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    LOG.info(snk.getReport().toJson());
    ReportManager.get().getReportable("foo1");
  }

  @Test
  public void testDFOChain() throws FlumeSpecException {
    ReportManager.get().clear();
    String spec = "agentDFOChain(\"foo:123\",\"bar\",\"baz\")";

    LOG.info("waled failchain: " + spec);
    new CompositeSink(new Context(), spec);
  }

}
