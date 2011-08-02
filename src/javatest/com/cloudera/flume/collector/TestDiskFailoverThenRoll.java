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
package com.cloudera.flume.collector;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Clock;

/**
 * This test does a disk failover and makes sure it does not fail on a
 * subsequent roller (like a collectorSink)
 */
public class TestDiskFailoverThenRoll {
  final public static Logger LOG = Logger
      .getLogger(TestDiskFailoverThenRoll.class);
  final MemorySinkSource mem = new MemorySinkSource();

  @Before
  public void replaceEscapedCustomDfsSink() {
    // Replace the null with a memory buffer that we have a reference to
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("null", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        LOG.info("excapedCustomDfs replaced with MemorySinkSource");
        mem.reset();
        return mem;
      }
    });
    FlumeBuilder.setSinkFactory(sf);
  }

  /**
   * This test builds a disk failover and then attempts to roll the output of
   * it. The diskFailover is set to retry every 1s (1000ms). We then check to
   * see if the number of elements has gone up for at most 3s.
   */
  @Test
  public void testAgentDFOCollector() throws IOException, FlumeSpecException,
      InterruptedException {
    String agentCollector = "{diskFailover(1000) => roll (100000) { null } }";
    Event e = new EventImpl("foo".getBytes());
    EventSink agent = FlumeBuilder.buildSink(
        LogicalNodeContext.testingContext(), agentCollector);
    agent.open();
    agent.append(e);

    for (int i = 0; i < 30; i++) {
      Clock.sleep(100);
      ReportEvent r = mem.getReport();
      LOG.info(r);
      if (r.getLongMetric("number of events") > 0) {
        return;
      }
    }
    fail("Test timed out, event didn't make it");
  }

}
