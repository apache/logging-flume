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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.durability.NaiveFileWALDeco;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * This tests end to end transmission and acks and recovery when there are
 * problems with the transmission.
 * 
 * Generally these tests are setup to trigger manually after 5 messages. Acking
 * prefixes and post fixes each batch with a ack begin and ack end message that
 * has info required for the checksum to add up. Thus 7 messages are sent per
 * chunk.
 * 
 * In the cases where there are retries, these are done using forceRetry
 * (instead of the time-dependent checkRetry)
 */
public class TestEndToEndAckFailureRecovery {

  public static Logger LOG = Logger
      .getLogger(TestEndToEndAckFailureRecovery.class);

  DirectMasterRPC mock;
  FlumeNode node;
  FlumeMaster master;

  /**
   * Setup mock clock, mock rpc master and a node that writes to a temp
   * directory.
   */
  @Before
  public void setUp() {
    System.out.println("=====");
    // Logger log = Logger.getLogger(NaiveFileWALManager.class.getName());
    Logger log = Logger.getRootLogger();
    log.setLevel(Level.DEBUG);

    File tmpdir = null;
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (IOException e) {
      fail("unable to create temp dir");
    }
    FlumeConfiguration.get().set(FlumeConfiguration.AGENT_LOG_DIR_NEW,
        tmpdir.getAbsolutePath());
    System.out.println("Writing out tempdir: " + tmpdir.getAbsolutePath());

    // This will register the FlumeNode with a MockMasterRPC so it doesn't go
    // across the network
    master = new FlumeMaster(FlumeConfiguration.get());
    mock = new DirectMasterRPC(master);
    node = new FlumeNode(mock, false /* starthttp */, false /* onshot */);

  }

  @After
  public void tearDown() {
    try {
      FileUtil.rmr(new File(FlumeConfiguration.get().get(
          FlumeConfiguration.AGENT_LOG_DIR_NEW)));
    } catch (IOException e) {
      LOG.error("Not supposed to happen: " + e.getMessage());
    }
  }

  /**
   * This case does the end to end ack successfully
   */
  @Test
  public void testAckSuccess() throws FlumeSpecException, IOException,
      InterruptedException {

    int count = 20;
    String rpt = "foo";
    String snk = "{ ackChecker => [ console, counter(\"" + rpt + "\") ] }";
    Context ctx = new ReportTestingContext();
    EventSink es = FlumeBuilder.buildSink(ctx, snk);
    // Excessively large timeout because I am manually triggering it.
    NaiveFileWALDeco<EventSink> wal = new NaiveFileWALDeco<EventSink>(ctx, es,
        node.getWalManager(), new TimeTrigger(new ProcessTagger(), 100000),
        node.getAckChecker().getAgentAckQueuer(), 1000);

    // open and send data.
    wal.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      wal.append(e);
      if (i % 5 == 4) {
        wal.rotate();
      }
    }

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    LOG.info("Last batch should be missing. " + count + " > " + ctr.getCount()
        + " sent");

    wal.close();
    LOG.info("All should make it through here. " + count + " =="
        + ctr.getCount() + " sent");
    assertEquals(20, ctr.getCount());

    // check master state -- there should be 5 pending acks.
    assertEquals(5, master.getAckMan().getPending().size());

    // after checking, there shuld be 0 acks left.
    node.getAckChecker().checkAcks();
    master.getAckMan().dumpLog();
    Set<String> pending2 = master.getAckMan().getPending();
    assertEquals(0, pending2.size());
  }

  /**
   * This case drops an ack section initializer
   */
  @Test
  public void testAckBeginFail() throws FlumeSpecException, IOException,
      InterruptedException {

    int count = 20;
    String rpt = "foo";

    // Phase I (20 events arrive, 3 groups make it)
    // Beg .. .. .. .. ..End
    // 00 01 02 03 04 05 06
    // 07 08 09 10 11 12 13
    // -- 15 16 17 18 19 20 (begin is missing)
    // 21 22 23 24 25 26 27

    // Phase II - (after a forced retry, 4 more sent for 24 total, but still no
    // good)
    // 14 XX 16 17 18 19 20 (retry fails, because of missing message)

    // Phase III - (another forced retry, 5 make it for 29 total and it is good
    // this time)
    // 14 15 16 17 18 19 20 (2nd retry attempt succeeds)

    // close

    String snk = "  { intervalDroppyAppend(15)  => { ackChecker => [ console, counter(\""
        + rpt + "\") ] } }  ";
    Context ctx = new ReportTestingContext();
    EventSink es = FlumeBuilder.buildSink(ctx, snk);

    NaiveFileWALDeco<EventSink> wal = new NaiveFileWALDeco<EventSink>(ctx, es,
        node.getWalManager(), new TimeTrigger(new ProcessTagger(), 100000),
        node.getAckChecker().getAgentAckQueuer(), 1000);
    wal.open();

    // // Phase I - (20 events arrive, 3 groups make it)
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      wal.append(e);
      if (i % 5 == 4) {
        wal.rotate();
      }
    }

    // arbitrary sleep for now.
    Clock.sleep(1000);

    // three blocks should have successfully made it.
    assertEquals(3, master.getAckMan().getPending().size()); // (g0, g1, g3)
    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    LOG.info(" Ack begin dropped, but all true messages went through. 20 == "
        + ctr.getCount() + " sent");
    // We dropped an ack begin message. Messages still make it through and there
    // should be some sort of ack warning on the master.
    assertEquals(20, ctr.getCount());

    // check master state
    master.getAckMan().dumpLog();
    Set<String> pending = master.getAckMan().getPending();
    assertEquals(3, pending.size());

    // check to make sure wal files are gone.
    node.getAckChecker().checkAcks();
    node.getAckChecker().checkRetry();
    master.getAckMan().dumpLog();
    pending = master.getAckMan().getPending();
    LOG.info("Number of pending acks (retry had failure)" + pending.size());
    assertEquals(0, pending.size());
    LOG.info("Event count 20  (one message missing/lost) ==" + ctr.getCount());
    assertEquals(20, ctr.getCount());

    Clock.sleep(1000); // somehow force other thread to go.

    // // Phase II - force message resent retry but one is dropped. (only 4 make
    // it)
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();

    Clock.sleep(500); // somehow force other thread to go.

    LOG.info("Was stuff resent? " + " 24 == " + ctr.getCount() + " sent");
    // This retry succeeds sending 4 more messages
    assertEquals(24, ctr.getCount());
    assertEquals(0, master.getAckMan().getPending().size());

    // // Phase III - force another retry
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();
    LOG
        .info("After another check. " + count + " < " + ctr.getCount()
            + " sent");
    Clock.sleep(500);

    // suceeded sending 5 more messages.
    assertEquals(29, ctr.getCount());
    assertEquals(1, master.getAckMan().getPending().size());

    // check the ack.
    node.getAckChecker().checkAcks();

    // we are clean.
    assertEquals(29, ctr.getCount());
    assertEquals(0, master.getAckMan().getPending().size());

    wal.close();
  }

  /**
   * This adds an interval Droppy between the ack writer and the ack checker.
   * This means a batch should fail and be retried.
   * 
   * All except for the internal wal writer to wal reader is serilaized. We
   * trigger the reader to read written files manaully (
   */
  @Test
  public void testMsgDropBehavior() throws FlumeSpecException,
      InterruptedException, IOException {

    int count = 20;
    String rpt = "foo";

    // // Phase I (19 message arrive, 3 groups ok).
    // Beg .. .. .. .. ..End
    // 00 01 02 03 04 05 06
    // 07 08 09 10 11 12 13
    // 14 -- 16 17 18 19 20 (drop a regular event message)
    // 21 22 23 24 25 26 27

    // // Phase II (4 more messages, 23 total, still only 3 groups)
    // 14 15 16 -- 18 19 20 (retry fails, because of missing message)

    // // Phase III (5 more message, 28 total, finally all 4 groups ok)
    // 14 15 16 17 18 19 20 (2nd retry attempt succeeds)

    // close

    String snk = "  { intervalDroppyAppend(16)  => { ackChecker => [ console, counter(\""
        + rpt + "\") ] } }  ";
    Context ctx = new ReportTestingContext();
    EventSink es = FlumeBuilder.buildSink(ctx, snk);

    NaiveFileWALDeco<EventSink> wal = new NaiveFileWALDeco<EventSink>(ctx, es,
        node.getWalManager(), new TimeTrigger(new ProcessTagger(), 100000),
        node.getAckChecker().getAgentAckQueuer(), 1000);
    wal.open();

    // // Phase I - (19 events arrive, 3 groups make it)
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      wal.append(e);
      if (i % 5 == 4) {
        wal.rotate();
      }
    }

    Clock.sleep(1000); // need other thread to make progress
    // 3 blocks should have successfully made it.
    assertEquals(3, master.getAckMan().getPending().size()); // (g0, g1, g3)

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    LOG.info(" Ack begin dropped, but all true message went through. 20 == "
        + ctr.getCount() + " sent");
    // We dropped a event message, so we should be down one, 19 of 20 events.
    assertEquals(19, ctr.getCount());

    // check master state
    master.getAckMan().dumpLog();
    Set<String> pending = master.getAckMan().getPending();
    assertEquals(3, pending.size());

    // // Phase II (4 more messages, 23 total, still only 3 groups)
    // check to make sure wal files are gone.
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();
    Clock.sleep(1000);
    master.getAckMan().dumpLog();
    pending = master.getAckMan().getPending();
    LOG.info("Number of pending acks (retry had failure)" + pending.size());
    assertEquals(0, pending.size());
    LOG.info("Event count 23  (one message missing/lost) ==" + ctr.getCount());
    assertEquals(23, ctr.getCount());

    // //Phase III (5 more messages, 28 total, 4 groups good)
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();
    Clock.sleep(1000); // somehow force other thread to go.

    LOG.info("Was stuff resent? " + " 28 == " + ctr.getCount() + " sent");
    // This retry succeeds sending 5 more messages
    assertEquals(28, ctr.getCount());
    Clock.sleep(1000); // somehow force other thread to go.
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();
    LOG.info("After another check. " + " 28 == " + ctr.getCount() + " sent");

    // Nothing happened
    assertEquals(28, ctr.getCount());
    assertEquals(0, master.getAckMan().getPending().size());
    wal.close();

  }

  @Test
  public void testAckEndFail() throws FlumeSpecException, IOException,
      InterruptedException {

    int count = 20;
    String rpt = "foo";

    // Phase I (20 message sent, 3 groups good, 1 group bad)
    // Beg .. .. .. .. ..End
    // 00 01 02 03 04 05 06
    // 07 08 09 10 11 12 13
    // 14 15 16 17 18 19 -- (drop an end ack group event message)
    // 21 22 23 24 25 26 27

    // Phase II (5 more messeage, retry succeeds).
    // 14 15 16 17 18 19 20 (retry succeeeds)

    // close

    String snk = "  { intervalDroppyAppend(21)  => { ackChecker => [ console, counter(\""
        + rpt + "\") ] } }  ";
    Context ctx = new ReportTestingContext();
    EventSink es = FlumeBuilder.buildSink(ctx, snk);

    // Big delay values so that test has to force different actions.
    NaiveFileWALDeco<EventSink> wal = new NaiveFileWALDeco<EventSink>(ctx, es,
        node.getWalManager(), new TimeTrigger(new ProcessTagger(), 100000),
        node.getAckChecker().getAgentAckQueuer(), 1000);
    wal.open();

    // Phase I (20 message sent, 3 groups good, 1 group bad)
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      wal.append(e);
      if (i % 5 == 4) {
        wal.rotate();
      }
    }

    Clock.sleep(1000); // need other thread to make progress
    // three blocks should have successfully made it.
    assertEquals(3, master.getAckMan().getPending().size()); // (g0, g1, g3)

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    LOG.info(" Ack begin dropped, but all true message went through. 20 == "
        + ctr.getCount() + " sent");
    // We dropped a event message, so we should be down one.
    assertEquals(20, ctr.getCount());

    // check master state
    Clock.sleep(1000); // somehow force other thread to go.
    master.getAckMan().dumpLog();
    Set<String> pending = master.getAckMan().getPending();
    assertEquals(3, pending.size());

    // Phase II (5 more messeage, retry succeeds).
    node.getAckChecker().checkAcks();
    node.getAckChecker().forceRetry();
    Clock.sleep(1000);
    master.getAckMan().dumpLog();
    pending = master.getAckMan().getPending();
    LOG.info("Number of pending acks (retry is success)" + pending.size());
    assertEquals(1, pending.size());
    LOG.info("Event count 25 (one message missing/lost) ==" + ctr.getCount());
    assertEquals(25, ctr.getCount());

    node.getAckChecker().checkAcks();
    Clock.sleep(1000); // somehow force other thread to go.

    LOG.info("Nothing new happened " + " 25 == " + ctr.getCount() + " sent");
    // This retry succeeds sending 5 more messages
    assertEquals(25, ctr.getCount());
    pending = master.getAckMan().getPending();
    assertEquals(0, pending.size());

    wal.close();
  }

  /**
   * This simulates if the collector fails to send an ack to the master, or if
   * the agent somehow fails to receive an ack sent to the master. The remedy
   * for both situations is the same -- resend the ack set that failed
   */
  @Test
  public void testMasterAckMissing() throws FlumeSpecException, IOException,
      InterruptedException {

    int count = 20;
    String rpt = "foo";
    String snk = "{ ackChecker => [ console, counter(\"" + rpt + "\") ] }";
    Context ctx = new ReportTestingContext();
    EventSink es = FlumeBuilder.buildSink(ctx, snk);
    NaiveFileWALDeco<EventSink> wal = new NaiveFileWALDeco<EventSink>(ctx, es,
        node.getWalManager(), new TimeTrigger(new ProcessTagger(), 100), node
            .getAckChecker().getAgentAckQueuer(), 10000);

    // open and send data.
    wal.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("test message " + i).getBytes());
      wal.append(e);
      if (i % 5 == 4) {
        wal.rotate();
      }
    }

    Clock.sleep(1000); // somehow force other thread to go.
    CounterSink ctr = (CounterSink) ReportManager.get().getReportable(rpt);
    LOG.info(" All should make it through here. " + count + " =="
        + ctr.getCount() + " sent");
    assertEquals(20, ctr.getCount());

    // check master state
    Clock.sleep(1000);
    master.getAckMan().dumpLog();

    Set<String> pending = master.getAckMan().getPending();
    assertEquals(4, pending.size());
    // acks should be at the master, but not check by the agent yet, thus 4
    // acks remaining.

    // To simulate the master not getting an ack message, we clear the tag on
    // the master. This will eventually force the child to retry
    // Artificially ack one so the agent doesn't get the ack.
    master.getAckMan().check(pending.iterator().next());

    node.getAckChecker().checkAcks();
    master.getAckMan().dumpLog();
    Set<String> pending2 = master.getAckMan().getPending();
    Clock.sleep(1000);
    // acks are checked so masters can drop state information
    assertEquals(0, pending2.size());
    assertEquals(1, node.getAckChecker().pending.size());

    // Do retry stuff.
    node.getAckChecker().forceRetry();
    Clock.sleep(1000);
    LOG.info("Expected 1 ack pending on master but instead had "
        + master.getAckMan().getPending().size());
    assertEquals(1, master.getAckMan().getPending().size());
    assertEquals(1, node.getAckChecker().pending.size());
    node.getAckChecker().checkAcks();
    assertEquals(0, master.getAckMan().getPending().size());
    assertEquals(0, node.getAckChecker().pending.size());
    wal.close();
  }

}
