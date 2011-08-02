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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.durability.NaiveFileWALDeco;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.Tagger;
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
  final static Logger LOG = Logger.getLogger(TestCollectorSink.class);

  @Before
  public void setUp() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
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

    try {
      // too many arguments
      String src4 = "collectorSink(\"file:///tmp/test\", \"bkjlasdf\", 1000, 1000)";
      FlumeBuilder.buildSink(new Context(), src4);
    } catch (Exception e) {
      return;
    }
    fail("unexpected fall through");
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
   */
  @Test
  public void testCorrectFilename() throws IOException, InterruptedException {
    CollectorSink sink = new CollectorSink(
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
        }, 250);

    sink.open();
    sink.append(new EventImpl(new byte[0]));
    sink.close();
    File f = new File("/tmp/flume-test-correct-filename/actual-file-tag");
    f.deleteOnExit();
    assertTrue("Expected filename does not exists " + f.getAbsolutePath(), f
        .exists());
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
  @SuppressWarnings("unchecked")
  public Pair<RollSink, EventSink> setupSink(FlumeNode node, File tmpdir)
      throws IOException, FlumeSpecException {

    // get collector and deconstruct it so we can control when it rolls (and
    // thus closes hdfs handles).
    String snkspec = "collectorSink(\"file:///" + tmpdir.getAbsolutePath()
        + "\",\"\")";
    CollectorSink coll = (CollectorSink) FlumeBuilder.buildSink(new Context(),
        snkspec);
    AckChecksumChecker<EventSink> chk = (AckChecksumChecker<EventSink>) coll
        .getSink();
    // insistent append
    EventSinkDecorator deco = (EventSinkDecorator<EventSink>) chk.getSink();
    // -> stubborn append
    deco = (EventSinkDecorator<EventSink>) deco.getSink();

    // stubborn append -> insistent
    deco = (EventSinkDecorator<EventSink>) deco.getSink();

    // insistent append -> mask
    deco = (EventSinkDecorator<EventSink>) deco.getSink();

    RollSink roll = (RollSink) deco.getSink();

    // normally inside wal
    NaiveFileWALDeco.AckChecksumRegisterer<EventSink> snk = new NaiveFileWALDeco.AckChecksumRegisterer(
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

    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();
          started.countDown();
          snk.append(e);
        } catch (IOException e1) {
          // could be an exception but we don't care.
          LOG.info("don't care about this exception: ", e1);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        done.countDown();
      }
    };
    t.start();
    boolean begun = started.await(60, TimeUnit.SECONDS);
    assertTrue("took too long to start", begun);
    snk.close();
    LOG.info("Interrupting appending thread");
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
  }

  /**
   * This tests close() and interrupt on a collectorSink in such a way that
   * close always happens after open started retrying.
   */
  @Test
  public void testHdfsDownInterruptAfterOpeningRetry()
      throws FlumeSpecException, IOException, InterruptedException {
    final EventSink snk = new LazyOpenDecorator(FlumeBuilder.buildSink(
        new Context(),
        "collectorSink(\"hdfs://nonexistant/user/foo\", \"foo\")"));

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);

    Thread t = new Thread("append thread") {
      public void run() {
        Event e = new EventImpl("foo".getBytes());
        try {
          snk.open();
          started.countDown();
          snk.append(e);
        } catch (IOException e1) {
          // could throw exception but we don't care
          LOG.info("don't care about this exception: ", e1);
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        done.countDown();
      }
    };
    t.start();
    boolean begun = started.await(60, TimeUnit.SECONDS);
    Clock.sleep(10);
    assertTrue("took too long to start", begun);
    snk.close();
    LOG.info("Interrupting appending thread");
    t.interrupt();
    boolean completed = done.await(60, TimeUnit.SECONDS);
    assertTrue("Timed out when attempting to shutdown", completed);
  }
}
