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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * Tests WriteAheadLogDeco's builder, multiple open close, and actual behavior.
 */
public class TestWriteAheadLogDecorator {
  static final Logger LOG = LoggerFactory
      .getLogger(TestWriteAheadLogDecorator.class);
  File tmpdir = null;

  @Before
  public void setUp() {
    // change config so that the write ahead log dir is in a new uniq place
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      Assert.fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    // This will register the FlumeNode with a MockMasterRPC so it doesn't go
    // across the network
    MockMasterRPC mock = new MockMasterRPC();
    @SuppressWarnings("unused")
    FlumeNode node = new FlumeNode(mock, false /* starthttp */, false /* oneshot */);
  }

  @After
  public void tearDown() {
    try {
      FileUtil.rmr(tmpdir);
    } catch (IOException e) {
      LOG.error("Failed to remove dir " + tmpdir, e);
    }
  }

  @Test
  public void testBuilder() throws FlumeSpecException {
    // need to have global state, so instantiated a mock master
    @SuppressWarnings("unused")
    FlumeNode node = new FlumeNode(new MockMasterRPC(), false, false);

    String cfg = " { ackedWriteAhead => null}";
    FlumeBuilder.buildSink(new Context(), cfg);

    String cfg1 = "{ ackedWriteAhead(15000) => null}";
    FlumeBuilder.buildSink(new Context(), cfg1);

    String cfg4 = "{ ackedWriteAhead(\"failurama\") => null}";
    try {
      FlumeBuilder.buildSink(new Context(), cfg4);
    } catch (Exception e) {
      return;
    }
    Assert.fail("unexpected fall through");
  }

  @Test
  public void testOpenClose() throws IOException, FlumeSpecException,
      InterruptedException {
    String rpt = "foo";
    String snk = " { ackedWriteAhead(100) => [console,  counter(\"" + rpt
        + "\") ] } ";
    for (int i = 0; i < 100; i++) {
      EventSink es = FlumeBuilder.buildSink(new Context(), snk);
      es.open();
      es.close();
    }

  }

  /**
   * This is a trickier test case. We create a console/counter sink that has a
   * ackedWriteAhead in front of it (aiming for 100 ms per batch). All events
   * should make it through after a slight delay.
   */
  @Test
  public void testBehavior() throws FlumeSpecException, InterruptedException,
      IOException {

    int count = 10;
    String rpt = "foo";
    String snk = " { ackedWriteAhead(500) => { ackChecker => [console,  counter(\""
        + rpt + "\") ] } } ";

    EventSink es = FlumeBuilder.buildSink(new ReportTestingContext(), snk);
    es.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      System.out.println("initial append: " + e);
      es.append(e);
      Clock.sleep(100);
    }
    Clock.sleep(5000);
    es.close();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    Assert.assertEquals(count, ctr.getCount());
  }
}
