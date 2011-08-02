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
package com.cloudera.flume.agent.durability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.SizeTrigger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.FileUtil;

/**
 * This tests the naive file WAL decorator.
 */
public class TestNaiveFileWALDeco {
  static final Logger LOG = Logger.getLogger(NaiveFileWALManager.class
      .getName());

  @Before
  public void setUp() {
    LOG.setLevel(Level.DEBUG);
  }

  /**
   * This test shows that we properly recover and eventually delete walfiles
   * from previous failed sessions.
   */
  @Test
  public void testRecoveredIsDeleted() throws IOException, FlumeSpecException,
      InterruptedException {

    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    // file with ack begin, data, and end messages
    File acked = new File("src/data/acked.00000000.20100204-015814430-0800.seq");
    // Assumes the NaiveFileWALManager!
    File writing = new File(new File(tmp, BenchmarkHarness.node
        .getPhysicalNodeName()), "writing");
    writing.mkdirs();

    // Must rename file because that name is in the meta data of the event
    // inside the file!
    // TODO (jon) make this restriction less strict in the future.

    FileUtil.dumbfilecopy(acked, new File(writing,
        "writeahead.00000000.20100204-015814F430-0800.seq"));

    // EventSource src = FlumeBuilder.buildSource("");
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "{ ackedWriteAhead => { ackChecker => counter(\"count\") } }");
    EventSource src = MemorySinkSource.cannedData("foo foo foo ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close(); // this should block until recovery complete.

    // agent checks for ack registrations.
    BenchmarkHarness.node.getAckChecker().checkAcks();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    // 1032 in file + 5 from silly driver
    assertEquals(1037, cnt.getCount());

    // check to make sure wal file is gone
    assertTrue(!new File(new File(tmp, "import"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "writing"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "logged"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "sending"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "sent"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "error"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "done"), acked.getName()).exists());

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * This specifically illustrates the case where an acktag doesn't match the
   * name of the wal file. I believe that we should just make the walfile match
   * its acktag an invariant.
   */
  @Test
  public void testMismatchedWalFilenameAckTag() throws IOException,
      FlumeSpecException, InterruptedException {
    // FlumeConfiguration.get().setLong(FlumeConfiguration.AGENT_LOG_MAX_AGE,
    // 50);
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    // file with ack begin, data, and end messages
    File acked = new File("src/data/acked.00000000.20100204-015814430-0800.seq");
    // Assumes the NaiveFileWALManager!
    File writing = new File(new File(tmp, BenchmarkHarness.node
        .getPhysicalNodeName()), "writing");
    writing.mkdirs();

    // /////////////////////
    // This illustrates the problems from the previous test.
    FileUtil.dumbfilecopy(acked, new File(writing, acked.getName()));
    // /////////////////////

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "{ ackedWriteAhead => { ackChecker => counter(\"count\") } }");
    EventSource src = MemorySinkSource.cannedData("foo foo foo ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close(); // this should block until recovery complete.

    // agent checks for ack registrations.
    BenchmarkHarness.node.getAckChecker().checkAcks();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    // 1032 in file + 5 from silly driverx
    assertEquals(1037, cnt.getCount());

    // check to make sure wal file is gone
    assertTrue(!new File(new File(tmp, "import"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "writing"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "logged"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "sending"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "error"), acked.getName()).exists());
    assertTrue(!new File(new File(tmp, "done"), acked.getName()).exists());

    // TODO (jon) is this the right behavior? I think assuming no name changes
    // locally is reasonable for now.

    assertTrue(new File(new File(new File(tmp, BenchmarkHarness.node
        .getPhysicalNodeName()), "sent"), acked.getName()).exists());

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * This "recovers" a truncated wal file as best as it can and moves it to the
   * err directory. Generally truncated files this would only happen on events
   * such as kill -9, power out, or out of disk space.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Test
  public void testRecoveredMovesToErr() throws IOException, FlumeSpecException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    // Assumes the NaiveFileWALManager!
    // file with ack begin, data and then truncated
    File truncated = new File(
        "src/data/truncated.00000000.20100204-015814430-0800.seq");
    File writing = new File(new File(tmp, BenchmarkHarness.node
        .getPhysicalNodeName()), "writing");

    writing.mkdirs();
    FileUtil.dumbfilecopy(truncated, new File(writing, truncated.getName()));

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "{ ackedWriteAhead => { ackChecker => counter(\"count\") } }");
    EventSource src = MemorySinkSource.cannedData("foo foo foo ", 5);
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close(); // this should block until recovery complete.

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    // 461 in file before truncated + 5 from silly driver
    assertEquals(466, cnt.getCount());

    // need to trigger ack checks..

    // BenchmarkHarness.mock.ackman.;

    // check to make sure wal file is gone
    File nodedir = new File(tmp, BenchmarkHarness.node.getPhysicalNodeName());

    assertTrue(!new File(new File(nodedir, "import"), truncated.getName())
        .exists());
    assertTrue(!new File(new File(nodedir, "writing"), truncated.getName())
        .exists());
    assertTrue(!new File(new File(nodedir, "logged"), truncated.getName())
        .exists());
    assertTrue(!new File(new File(nodedir, "sending"), truncated.getName())
        .exists());
    assertTrue(!new File(new File(nodedir, "sent"), truncated.getName())
        .exists());
    assertTrue(new File(new File(nodedir, "error"), truncated.getName())
        .exists());
    assertTrue(!new File(new File(nodedir, "done"), truncated.getName())
        .exists());

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * An old implementation could deadlock if append was called before open. This
   * test tests this issue to an extent - we don't test concurrent append-> open
   * as it requires injecting code into the implementation of append.
   */
  @Test
  public void testAppendBeforeOpen() throws InterruptedException {
    final NaiveFileWALDeco<EventSink> d = new NaiveFileWALDeco<EventSink>(
        new Context(), new NullSink(),
        new NaiveFileWALManager(new File("/tmp")), new SizeTrigger(0, null),
        new AckListener.Empty(), 1000000);
    final CountDownLatch cdl1 = new CountDownLatch(1);
    new Thread() {
      public void run() {
        try {
          d.append(new EventImpl("hello".getBytes()));
          d.open();
          cdl1.countDown();
        } catch (IOException e) {
          LOG.error("Saw IOException in testAppendBeforeOpen", e);
        } catch (IllegalStateException e) {
          // Expected illegal state exception due to not being open
          cdl1.countDown();
        }
      }
    }.start();
    assertTrue("Latch not fired", cdl1.await(5000, TimeUnit.MILLISECONDS));
  }

  /**
   * Test the case where data with out ack tags is being sent to the wal deco.
   */
  @Test
  public void testBadRegistererAppend() throws InterruptedException {

    final NaiveFileWALDeco<EventSink> d = new NaiveFileWALDeco<EventSink>(
        new Context(), new NullSink(),
        new NaiveFileWALManager(new File("/tmp")), new SizeTrigger(0, null),
        new AckListener.Empty(), 1000000);

    final CountDownLatch cdl1 = new CountDownLatch(1);
    new Thread() {
      public void run() {
        try {
          d.append(new EventImpl("hello".getBytes()));
          d.open();
          cdl1.countDown();
        } catch (IOException e) {
          LOG.error("Saw IOException in testAppendBeforeOpen", e);
        } catch (IllegalStateException e) {
          // Expected illegal state exception due to not being open
          cdl1.countDown();
        }
      }
    }.start();
    assertTrue("Latch not fired", cdl1.await(5000, TimeUnit.MILLISECONDS));
  }

  @Test(expected = IOException.class)
  public void testExceptionThreadHandoff() throws IOException {
    try {
      BenchmarkHarness.setupLocalWriteDir();
      Event e = new EventImpl(new byte[0]);
      EventSink snk = new EventSink.Base() {
        @Override
        public void append(Event e) throws IOException {
          throw new IOException("mock ioe");
        }
      };

      FlumeNode node = FlumeNode.getInstance();
      EventSinkDecorator<EventSink> deco = new NaiveFileWALDeco<EventSink>(
          new Context(), snk, node.getWalManager(), new TimeTrigger(
              new ProcessTagger(), 1000), node.getAckChecker()
              .getAgentAckQueuer(), 1000000);

      deco.open();
      deco.append(e);
      deco.close();
    } catch (IOException e) {
      throw e;
    } finally {
      BenchmarkHarness.cleanupLocalWriteDir();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadE2eBuilderArgs() {
    SinkDecoBuilder b = NaiveFileWALDeco.builderEndToEndDir();
    b.build(new Context(), "foo", "bar");
  }

}
