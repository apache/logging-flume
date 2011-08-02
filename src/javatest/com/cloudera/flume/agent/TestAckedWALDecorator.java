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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.FileUtil;

/**
 * Tests WriteAheadLogDeco's builder, multiple open close, and actual behavior.
 */
public class TestAckedWALDecorator {
  final static Logger LOG = Logger.getLogger(TestAckedWALDecorator.class);

  File tmpdir = null;
  FlumeNode node;
  MockMasterRPC mock;

  @Before
  public void setUp() {
    // change config so that the write ahead log dir is in a new uniq place
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    // This will register the FlumeNode with a MockMasterRPC so it doesn't go
    // across the network
    mock = new MockMasterRPC();
    node = new FlumeNode(mock, false /* starthttp */, false /* oneshot */);
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
    fail("unexpected fall through");
  }

  @Test
  public void testOpenClose() throws IOException, FlumeSpecException {
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

    int count = 100;
    String rpt = "foo";
    // in a real situation, there would be a agent sink after the
    // ackedWriteAhead and a collectorSource before the ackChecker block.
    String snk = " { ackedWriteAhead(1000) => { ackChecker => counter(\"" + rpt
        + "\") } }  ";

    EventSink es = FlumeBuilder.buildSink(new ReportTestingContext(), snk);
    es.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      es.append(e);
    }

    // last batch doesn't flush automatically unless time passes or closed-- one
    // pending
    Set<String> pending = mock.ackman.getPending();
    assertEquals(0, pending.size());

    // close and flush
    es.close();
    node.getAckChecker().checkAcks();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    assertEquals(count, ctr.getCount());

    // check master state
    mock.ackman.dumpLog();
    Set<String> pending2 = mock.ackman.getPending();
    assertEquals(0, pending2.size());
  }

  /**
   * This test case does something to force a retransmit attempt
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testForceRetransmit() throws FlumeSpecException, IOException {
    // This will register the FlumeNode with a MockMasterRPC so it doesn't go
    // across the network
    MockMasterRPC mock = new MockMasterRPC();
    FlumeNode node = new FlumeNode(mock, false /* starthttp */, false /* onshot */);
    AckListener pending = node.getAckChecker().getAgentAckQueuer();

    // initial source of data.
    MemorySinkSource mem = new MemorySinkSource();
    byte[] tag = "my tag".getBytes();
    AckChecksumInjector<EventSink> ackinj = new AckChecksumInjector<EventSink>(
        mem, tag, pending);
    ackinj.open();
    for (int i = 0; i < 5; i++) {
      ackinj.append(new EventImpl(("Event " + i).getBytes()));
    }
    ackinj.close();

    // there now are now 7 events in the mem
    // consume data.
    EventSink es1 = new ConsoleEventSink();
    EventUtil.dumpAll(mem, es1);
    System.out.println("---");

    String rpt = "foo";
    String snk = "  { intervalDroppyAppend(5)  => { ackChecker => [console, counter(\""
        + rpt + "\") ] } }  ";
    EventSink es = FlumeBuilder.buildSink(new Context(), snk);
    mem.open(); // resets index.
    es.open();
    EventUtil.dumpAll(mem, es);
    node.getAckChecker().checkAcks();
    assertEquals(1, node.getAckChecker().pending.size());

    mem.open(); // resets index.
    // send to collector, collector updates master state (bypassing flakeyness)
    EventUtil.dumpAll(mem, ((EventSinkDecorator<EventSink>) es).getSink());
    // agent gets state from master, updates it ack states
    try {
      node.getAckChecker().checkAcks();
    } catch (RuntimeException npe) {
      // this actually goes an trys to delete a file, but since this is fake it
      // doesn't exist
      LOG.info(npe.getMessage());
    }

    assertEquals(0, node.getAckChecker().pending.size());
  }
}
