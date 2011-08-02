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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.durability.NaiveFileWALDeco;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeArgException;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.Pair;

/**
 * This tests the builder and makes sure we can close a collector properly when
 * interrupted.
 * 
 * TODO This should, but does not, test situations where the collectorSink
 * actually connects to an HDFS namenode, and then recovers from when an actual
 * HDFS goes down and comes back up. Instead this contains tests that shows when
 * a HDFS connection is fails, the retry metchanisms are forced to exit.
 */
public class TestCollectorSink {
  final static Logger LOG = LoggerFactory.getLogger(TestCollectorSink.class);

  @Before
  public void setUp() {
    // Log4j specific settings for debugging.
    org.apache.log4j.Logger.getLogger(CollectorSink.class)
        .setLevel(Level.DEBUG);
    org.apache.log4j.Logger.getLogger(AckChecksumChecker.class).setLevel(
        Level.DEBUG);
    org.apache.log4j.Logger.getLogger(CustomDfsSink.class).setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger(EscapedCustomDfsSink.class).setLevel(
        Level.WARN);
  }

  @Test
  public void testBuilder() throws FlumeSpecException {
    Exception ex = null;
    try {
      String src = "collectorSink";
      FlumeBuilder.buildSink(new Context(), src);
    } catch (Exception e) {
      return;
    }
    assertNotNull("No exception thrown!", ex != null);

    // dir / filename
    String src2 = "collectorSink(\"file:///tmp/test\", \"testfilename\")";
    FlumeBuilder.buildSink(new Context(), src2);

    // millis
    String src3 = "collectorSink(\"file:///tmp/test\", \"testfilename\", 1000)";
    FlumeBuilder.buildSink(new Context(), src3);
  }

  @Test(expected = FlumeArgException.class)
  public void testBuilderFail() throws FlumeSpecException {
    // too many arguments
    String src4 = "collectorSink(\"file:///tmp/test\", \"bkjlasdf\", 1000, 1000)";
    FlumeBuilder.buildSink(new Context(), src4);
  }

  /**
   * play evil games with escape characters, and check that that old syntax
   * works with old semantics.
   */
  @Test
  public void testBuilderHandleEvilSlashes() throws FlumeSpecException {

    String src4 = "collectorSink(\"file://C:\\tmp\\test\", \"file\", 1000)";
    CollectorSink snk = (CollectorSink) FlumeBuilder.buildSink(new Context(),
        src4);
    assertEquals(
        "escapedCustomDfs(\"file://C:\\tmp\\test\",\"file%{rolltag}\" )",
        snk.roller.getRollSpec());
  }

  @Test
  public void testOpenClose() throws FlumeSpecException, IOException,
      InterruptedException {
    String src2 = "collectorSink(\"file:///tmp/test\",\"testfilename\")";

    for (int i = 0; i < 100; i++) {
      EventSink snk = FlumeBuilder.buildSink(new Context(), src2);
      snk.open();
      snk.close();
    }
  }

  /**
   * Test that file paths are correctly constructed from dir + path + tag
   * 
   * @throws InterruptedException
   * @throws FlumeSpecException
   */
  @Test
  public void testCorrectFilename() throws IOException, InterruptedException,
      FlumeSpecException {
    CollectorSink sink = new CollectorSink(new Context(),
        "file:///tmp/flume-test-correct-filename", "actual-file-", 10000,
        new Tagger() {
          public String getTag() {
            return "tag";
          }

          public String newTag() {
            return "tag";
          }

          public Date getDate() {
            return new Date();
          }

          public void annotate(Event e) {

          }
        }, 250, FlumeNode.getInstance().getCollectorAckListener());

    sink.open();
    sink.append(new EventImpl(new byte[0]));
    sink.close();
    File f = new File("/tmp/flume-test-correct-filename/actual-file-tag");
    f.deleteOnExit();
    assertTrue("Expected filename does not exists " + f.getAbsolutePath(),
        f.exists());
  }

  /**
   * Setup a data set with acks in the stream. This simulates data coming from
   * an agent expecting end-to-end acks.
   * 
   * @throws InterruptedException
   */
  MemorySinkSource setupAckRoll() throws IOException, InterruptedException {

    // we can roll now.
    MemorySinkSource ackedmem = new MemorySinkSource();
    // simulate the stream coming from an e2e acking agent.
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(
        ackedmem);
    inj.open();
    inj.append(new EventImpl("foo 1".getBytes()));
    inj.close();

    inj = new AckChecksumInjector<EventSink>(ackedmem);
    inj.open();
    inj.append(new EventImpl("foo 2".getBytes()));
    inj.close();

    inj = new AckChecksumInjector<EventSink>(ackedmem);
    inj.open();
    inj.append(new EventImpl("foo 3".getBytes()));
    inj.close();
    return ackedmem;
  }

  /**
   * Construct a sink that will process the acked stream.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  public Pair<RollSink, EventSink> setupSink(FlumeNode node, File tmpdir)
      throws IOException, FlumeSpecException {

    // get collector and deconstruct it so we can control when it rolls (and
    // thus closes hdfs handles).
    String snkspec = "collectorSink(\"file:///" + tmpdir.getAbsolutePath()
        + "\",\"\")";
    CollectorSink coll = (CollectorSink) FlumeBuilder.buildSink(new Context(),
        snkspec);
    RollSink roll = coll.roller;

    // normally inside wal
    NaiveFileWALDeco.AckChecksumRegisterer<EventSink> snk = new NaiveFileWALDeco.AckChecksumRegisterer<EventSink>(
        coll, node.getAckChecker().getAgentAckQueuer());
    return new Pair<RollSink, EventSink>(roll, snk);
  }

  /**
   * We need to make sure the ack doesn't get sent until the roll happens.
   */
  @Test
  public void testIdealAckOnRoll() throws IOException, FlumeSpecException,
      InterruptedException {
    // we don't care about the durability parts of the walMan, only the ack
    // parts. Normally this manager would want to delete a wal file (or wal
    // entries). This stubs that out to a call doesn't cause a file not found
    // exception.
    WALManager mockWalMan = mock(WALManager.class);
    BenchmarkHarness.setupFlumeNode(null, mockWalMan, null, null, null);
    FlumeNode node = FlumeNode.getInstance();
    File tmpdir = FileUtil.mktempdir();

    EventSource ackedmem = setupAckRoll();
    Pair<RollSink, EventSink> p = setupSink(node, tmpdir);
    EventSink snk = p.getRight();
    RollSink roll = p.getLeft();
    snk.open();

    String tag1 = roll.getCurrentTag();
    LOG.info(tag1);
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    // don't rotate the first one.
    assertEquals(1, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    // still one ack pending.
    assertEquals(1, node.getAckChecker().getPendingAckTags().size());

    String tag2 = roll.getCurrentTag();
    LOG.info(tag2);
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate();
    // two acks pending.
    assertEquals(2, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    // no more acks pending.
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    String tag3 = roll.getCurrentTag();
    LOG.info(tag3);
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate();
    // one ack pending
    assertEquals(1, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    // no more acks pending.
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    snk.close();

    FileUtil.rmr(tmpdir);
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * We need to make sure the ack doesn't get sent until the roll happens.
   * 
   * This one does unalighned acks where the first event after a rotation is an
   * ack end.
   */
  @Test
  public void testUnalignedAckOnRollEndBoundary() throws IOException,
      FlumeSpecException, InterruptedException {
    // we don't care about the durability parts of the walMan, only the ack
    // parts. Normally this manager would want to delete a wal file (or wal
    // entries). This stubs that out to a call doesn't cause a file not found
    // exception.
    WALManager mockWalMan = mock(WALManager.class);
    BenchmarkHarness.setupFlumeNode(null, mockWalMan, null, null, null);
    FlumeNode node = FlumeNode.getInstance();
    File tmpdir = FileUtil.mktempdir();

    EventSource ackedmem = setupAckRoll();
    Pair<RollSink, EventSink> p = setupSink(node, tmpdir);
    EventSink snk = p.getRight();
    RollSink roll = p.getLeft();
    snk.open();

    String tag1 = roll.getCurrentTag();
    LOG.info(tag1);
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate(); // we should have the first batch and part of the second
    // one ack pending
    assertEquals(1, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    // no acks pending
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    // note, we still are checking state for the 2nd batch of messages

    String tag2 = roll.getCurrentTag();
    LOG.info(tag2);
    // This is the end msg closes the 2nd batch
    snk.append(ackedmem.next()); // ack end
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate();
    // now 2nd batch and 3rd batch are pending.
    assertEquals(2, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();

    // no more acks out standing
    LOG.info("pending ack tags: " + node.getAckChecker().getPendingAckTags());
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    snk.close();

    FileUtil.rmr(tmpdir);
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * We need to make sure the ack doesn't get sent until the roll happens.
   */
  @Test
  public void testUnalignedAckOnRoll() throws IOException, FlumeSpecException,
      InterruptedException {
    // we don't care about the durability parts of the walMan, only the ack
    // parts. Normally this manager would want to delete a wal file (or wal
    // entries). This stubs that out to a call doesn't cause a file not found
    // exception.
    WALManager mockWalMan = mock(WALManager.class);
    BenchmarkHarness.setupFlumeNode(null, mockWalMan, null, null, null);
    FlumeNode node = FlumeNode.getInstance();
    File tmpdir = FileUtil.mktempdir();

    EventSource ackedmem = setupAckRoll();
    Pair<RollSink, EventSink> p = setupSink(node, tmpdir);
    EventSink snk = p.getRight();
    RollSink roll = p.getLeft();
    snk.open();

    String tag1 = roll.getCurrentTag();
    LOG.info(tag1);
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    snk.append(ackedmem.next()); // ack beg
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate(); // we should have the first batch and part of the second
    // one ack pending
    assertEquals(1, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    // no acks pending
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    // we are partially through the second batch, at a different split point

    String tag2 = roll.getCurrentTag();
    LOG.info(tag2);
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    snk.append(ackedmem.next()); // ack beg
    snk.append(ackedmem.next()); // data
    snk.append(ackedmem.next()); // ack end
    Clock.sleep(10); // have to make sure it is not in the same millisecond
    roll.rotate();
    // now we have closed off group2 and group3
    assertEquals(2, node.getAckChecker().getPendingAckTags().size());
    node.getAckChecker().checkAcks();
    Clock.sleep(10); // have to make sure it is not in the same millisecond

    // no more acks left
    LOG.info("pending ack tags: " + node.getAckChecker().getPendingAckTags());
    assertEquals(0, node.getAckChecker().getPendingAckTags().size());

    snk.close();

    FileUtil.rmr(tmpdir);
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * This tests close() and interrupt on a collectorSink in such a way that
   * close can happen before open has completed.
   */
  @Test
  public void testHdfsDownInterruptBeforeOpen() throws FlumeSpecException,
      IOException, InterruptedException {
    final EventSink snk = FlumeBuilder.buildSink(new Context(),
        "collectorSink(\"hdfs://nonexistant/user/foo\", \"foo\")");

    final CountDownLatch done = new CountDownLatch(1);

    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();

          snk.append(e);
        } catch (IOException e1) {
          // could be exception but we don't care
          LOG.info("don't care about this exception: ", e1);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        done.countDown();
      }
    };
    t.start();
    snk.close();
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
  }

  /**
   * This tests close() and interrupt on a collectorSink in such a way that
   * close always happens after open has completed.
   */
  @Test
  public void testHdfsDownInterruptAfterOpen() throws FlumeSpecException,
      IOException, InterruptedException {
    final EventSink snk = FlumeBuilder.buildSink(new Context(),
        "collectorSink(\"hdfs://nonexistant/user/foo\", \"foo\")");

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Exception> are = new AtomicReference(null);
    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();
          started.countDown();
          snk.append(e);
        } catch (Exception e1) {
          // could be an exception but we don't care.
          LOG.info("don't care about this exception: ", e1);
          are.set(e1);
        }
        done.countDown();
      }
    };
    t.start();
    boolean begun = started.await(60, TimeUnit.SECONDS);
    assertTrue("took too long to start", begun);

    // there is a race between this close call and the append call inside the
    // thread. In this test we only want to verify that this exits in a
    // reasonable amount of time.

    snk.close();
    LOG.info("Interrupting appending thread");
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
  }

  /**
   * This tests close() and interrupt on a collectorSink in such a way that
   * close happens while a append call is blocked.
   */
  @Test
  public void testHdfsDownInterruptBlockedAppend() throws FlumeSpecException,
      IOException, InterruptedException {
    final EventSink snk = FlumeBuilder.buildSink(new Context(),
        "collectorSink(\"hdfs://nonexistant/user/foo\", \"foo\")");

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);

    final AtomicReference<Exception> are = new AtomicReference(null);
    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();
          started.countDown();
          snk.append(e);
        } catch (Exception e1) {
          e1.printStackTrace();
          are.set(e1);
        }
        done.countDown();
      }
    };
    t.start();
    boolean begun = started.await(60, TimeUnit.SECONDS);
    assertTrue("took too long to start", begun);

    // there is a race between this close call and the append call inside the
    // thread. This sleep call should give enough to cause the append to get
    // stuck.
    Clock.sleep(1000);

    snk.close();
    LOG.info("Interrupting appending thread");
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
    assertTrue("Expected exit due to interrupted exception",
        are.get() instanceof InterruptedException);
  }

  /**
   * This tests close() and interrupt on a collectorSink in such a way that
   * close always happens after open started retrying.
   */
  @Test
  public void testHdfsDownInterruptAfterOpeningRetry()
      throws FlumeSpecException, IOException, InterruptedException {
    final EventSink snk = new LazyOpenDecorator<EventSink>(
        FlumeBuilder.buildSink(new Context(),
            "collectorSink(\"hdfs://nonexistant/user/foo\", \"foo\")"));

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);

    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();
          started.countDown();
          snk.append(e); // this blocks until interrupted.

        } catch (IOException e1) {
          // could throw exception but we don't care
          LOG.info(
              "don't care about this IO exception (will eventually be interrupted exception): ",
              e1);
        } catch (InterruptedException e1) {
          LOG.info("don't care about this exception: ", e1);
          e1.printStackTrace();
        }
        done.countDown();
      }
    };
    t.start();
    boolean begun = started.await(60, TimeUnit.SECONDS);
    assertTrue("took too long to start", begun);
    Clock.sleep(1000);

    // If this close happens before the append, test hangs.
    // problem - append needs to be happening before next
    // close happens, need to sleep

    snk.close();

    // if this finishes all the way (all threads, the we are ok).
    LOG.info("Interrupting appending thread");
    // Clock.sleep(5000);
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
  }

  /**
   * Unless the internal ack map is guarded by locks, a collector sink could
   * cause ConcurrentModificationExceptions.
   * 
   * The collection gets modified by the stream writing acks into the map, and
   * then by the periodic close call from another thread that flushes it.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   * @throws InterruptedException
   */
  @Test
  public void testNoConcurrentModificationOfAckMapException()
      throws IOException, FlumeSpecException, InterruptedException {
    File dir = FileUtil.mktempdir();
    try {
      // set to 1 and 10 when debugging
      final int COUNT = 100;
      final int ROLLS = 1000;

      // setup a source of data that will shove a lot of ack laden data into the
      // stream.
      MemorySinkSource mem = new MemorySinkSource();
      EventSource src = new NoNlASCIISynthSource(COUNT, 10);
      src.open();
      Event e;
      while ((e = src.next()) != null) {
        AckChecksumInjector<EventSink> acks = new AckChecksumInjector<EventSink>(
            mem);
        acks.open(); // memory in sink never resets, and just keeps appending
        acks.append(e);
        acks.close();
      }

      class TestAckListener implements AckListener {
        long endCount, errCount, expireCount, startCount;
        Set<String> ends = new HashSet<String>();

        @Override
        synchronized public void end(String group) throws IOException {
          endCount++;
          ends.add(group);
          LOG.info("End count incremented to " + endCount);
        }

        @Override
        synchronized public void err(String group) throws IOException {
          errCount++;
        }

        @Override
        synchronized public void expired(String key) throws IOException {
          expireCount++;
        }

        @Override
        synchronized public void start(String group) throws IOException {
          startCount++;
        }

      }
      ;

      TestAckListener fakeMasterRpc = new TestAckListener();

      // massive roll millis because the test will force fast and frequent rolls
      CollectorSink cs = new CollectorSink(LogicalNodeContext.testingContext(),
          "file:///" + dir.getAbsolutePath(), "test", 1000000,
          new ProcessTagger(), 250, fakeMasterRpc) {
        @Override
        public void append(Event e) throws IOException, InterruptedException {
          LOG.info("Pre  append: "
              + e.getAttrs().get(AckChecksumInjector.ATTR_ACK_HASH));
          super.append(e);
          LOG.info("Post append: "
              + e.getAttrs().get(AckChecksumInjector.ATTR_ACK_HASH));
        }
      };

      // setup a roller that will roll like crazy from a separate thread
      final RollSink roll = cs.roller;
      Thread t = new Thread("roller") {
        @Override
        public void run() {
          try {
            for (int i = 0; i < ROLLS; i++) {
              roll.rotate();
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      };
      t.start();

      // pump it through and wait for exception.
      cs.open();

      EventUtil.dumpAll(mem, cs);
      t.join();

      cs.close();
      long rolls = roll.getMetrics().getLongMetric(RollSink.A_ROLLS);
      LOG.info("rolls {} ", rolls);
      LOG.info("start={} end={}", fakeMasterRpc.startCount,
          fakeMasterRpc.endCount);
      LOG.info("endset size={}", fakeMasterRpc.ends.size());
      LOG.info("expire={} err={}", fakeMasterRpc.expireCount,
          fakeMasterRpc.errCount);
      assertEquals(ROLLS, rolls);
      assertEquals(0, fakeMasterRpc.startCount);
      assertEquals(COUNT, fakeMasterRpc.ends.size());
      assertEquals(0, fakeMasterRpc.expireCount);
      assertEquals(0, fakeMasterRpc.errCount);

    } finally {
      FileUtil.rmr(dir);
    }
  }

  @Test
  public void testMultipleSinks() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "collector(5000) { [ counter(\"foo\"), counter(\"bar\") ] }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(
        LogicalNodeContext.testingContext()), spec);
    snk.open();
    snk.append(new EventImpl("this is a test".getBytes()));
    snk.close();
    ReportEvent rpta = ReportManager.get().getReportable("foo").getMetrics();
    assertEquals(1, (long) rpta.getLongMetric("foo"));
    ReportEvent rptb = ReportManager.get().getReportable("bar").getMetrics();
    assertEquals(1, (long) rptb.getLongMetric("bar"));
  }
}
