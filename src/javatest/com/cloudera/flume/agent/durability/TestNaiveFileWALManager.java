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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.FileUtil;

public class TestNaiveFileWALManager {
  // has 5 good entries.
  final static String WAL_OK = "src/data/hadoop_logs_5.hdfs";
  public static Logger LOG = Logger.getLogger(TestNaiveFileWALManager.class);

  @Before
  public void setUp() {
    LOG.setLevel(Level.DEBUG);
  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */
  @Test
  public void testImport() throws IOException {

    File dir = FileUtil.mktempdir();
    // putting in large ridiculous constant
    NaiveFileWALManager wal = new NaiveFileWALManager(dir);
    wal.open();

    File logdir = new File(dir, NaiveFileWALManager.IMPORTDIR);
    File src = new File(WAL_OK);
    File dest = new File(logdir, "ok.0000000.20091104-101213997-0800.seq");
    FileUtil.dumbfilecopy(src, dest);
    dest.deleteOnExit();

    assertEquals(0, wal.getWritingTags().size());
    assertEquals(0, wal.getLoggedTags().size());
    assertEquals(0, wal.getSendingTags().size());
    assertEquals(0, wal.getSentTags().size());

    wal.importData();

    assertEquals(0, wal.getWritingTags().size());
    assertEquals(1, wal.getLoggedTags().size());
    assertEquals(0, wal.getSendingTags().size());
    assertEquals(0, wal.getSentTags().size());

    wal.stopDrains();
  }

  @Test
  public void testTransitions() throws IOException, InterruptedException {
    File dir = FileUtil.mktempdir();
    // putting in large ridiculous constant
    NaiveFileWALManager wal = new NaiveFileWALManager(dir);
    wal.open();

    // get a sink write to it and then be done with it.
    Tagger t = new ProcessTagger();
    EventSink sink = wal.newWritingSink(t);
    sink.open();
    sink.append(new EventImpl("foo".getBytes()));
    assertEquals(1, wal.getWritingTags().size());

    // close moves to logged state.
    sink.close();
    assertEquals(0, wal.getWritingTags().size());
    assertEquals(1, wal.getLoggedTags().size());

    // logged values can transition to the sending state by getting a sender.
    EventSource curSource = wal.getUnackedSource();
    // no state change
    assertEquals(0, wal.getWritingTags().size());
    assertEquals(0, wal.getLoggedTags().size());
    assertEquals(1, wal.getSendingTags().size());

    // open changes state
    curSource.open();
    assertEquals(0, wal.getLoggedTags().size());
    assertEquals(1, wal.getSendingTags().size());

    // read next event
    Event e = null;
    ConsoleEventSink console = new ConsoleEventSink();
    while ((e = curSource.next()) != null) {
      console.append(e);
    }
    curSource.close();
    assertEquals(0, wal.getSendingTags().size());
    assertEquals(1, wal.getSentTags().size());

    // trigger to make it move from sent to acked/done
    String tag = wal.getSentTags().iterator().next();
    wal.toAcked(tag);
    assertEquals(0, wal.getSentTags().size());
  }

  /**
   * This test puts a file in each log dir and makes sure they are all
   * recovered.
   */
  @Test
  public void testRecovers() throws IOException, FlumeSpecException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    // putting in large ridiculous constant
    NaiveFileWALManager wal = new NaiveFileWALManager(tmp);
    wal.open(); // create dirs

    File acked = new File(WAL_OK); // ok but unframed

    // copy files and then recover them.
    FileUtil.dumbfilecopy(acked, new File(wal.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(wal.loggedDir,
        "logged.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(wal.sendingDir,
        "sending.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(wal.sentDir,
        "send.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(wal.importDir,
        "import.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(wal.errorDir,
        "error.00000000.20100204-015814F430-0800.seq"));
    wal.recover();

    // check to make sure wal file is gone
    // assertTrue(new File(tmp, "import").list().length == 0);
    assertEquals(0, new File(tmp, "writing").list().length);
    assertEquals(0, new File(tmp, "sending").list().length);
    assertEquals(0, new File(tmp, "sent").list().length);
    assertEquals(0, new File(tmp, "done").list().length);
    // pre-existing error, and writing didn't have proper ack wrappers
    assertEquals(4, new File(tmp, "error").list().length);
    // logged, writing, sending, sent
    assertEquals(4, new File(tmp, "logged").list().length);

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * This test puts a file in each log dir and makes sure they are all
   * recovered. This test has no closing ack event.
   */
  @Test
  public void testReframeRecovers() throws IOException, FlumeSpecException,
      InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    NaiveFileWALManager wal = new NaiveFileWALManager(tmp);
    wal.open(); // create dirs

    // create a seq file with no ack close.
    File f = new File(wal.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq");
    SeqfileEventSink sf = new SeqfileEventSink(f);
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(sf);
    inj.open();
    inj.append(new EventImpl("test".getBytes()));
    // notice ack checksum inj not closed, but the subsink is.
    // no ack end event sent on purpose, which forces recover() to reframe the
    // data.
    sf.close();

    // do the low level recovery
    wal.recover();

    // check to make sure wal file is gone
    // assertTrue(new File(tmp, "import").list().length == 0);
    assertEquals(0, new File(tmp, "writing").list().length);
    assertEquals(0, new File(tmp, "sending").list().length);
    assertEquals(0, new File(tmp, "sent").list().length);
    assertEquals(0, new File(tmp, "done").list().length);
    assertEquals(1, new File(tmp, "error").list().length);
    assertEquals(1, new File(tmp, "logged").list().length);

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * Multiple ack-starts should cause problem that gets cleaned up.
   */
  @Test
  public void testReframeMultipleOpenAcks() throws IOException,
      InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    NaiveFileWALManager wal = new NaiveFileWALManager(tmp);
    wal.open(); // create dirs

    // create a seq file with no ack close.
    File f = new File(wal.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq");
    SeqfileEventSink sf = new SeqfileEventSink(f);
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(sf);
    inj.open();
    inj.append(new EventImpl("test".getBytes()));

    // Cause a state ack check state error
    Event e = inj.openEvent();
    sf.append(e);

    // proper close event
    inj.close();

    // do the low level recovery
    wal.recover();

    // check to make sure wal file is gone
    // assertTrue(new File(tmp, "import").list().length == 0);
    assertEquals(0, new File(tmp, "writing").list().length);
    assertEquals(0, new File(tmp, "sending").list().length);
    assertEquals(0, new File(tmp, "sent").list().length);
    assertEquals(0, new File(tmp, "done").list().length);
    assertEquals(1, new File(tmp, "error").list().length);
    assertEquals(1, new File(tmp, "logged").list().length);

    BenchmarkHarness.cleanupLocalWriteDir();

  }

  /**
   * This reframes data that has no ack-start and no ack-end.
   */
  @Test
  public void testReframeUnframed() throws IOException, InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    NaiveFileWALManager wal = new NaiveFileWALManager(tmp);
    wal.open(); // create dirs

    // create a seq file with no ack close or ack open
    File f = new File(wal.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq");
    SeqfileEventSink sf = new SeqfileEventSink(f);
    sf.open();
    sf.append(new EventImpl("test".getBytes()));
    sf.close();

    // do the low level recovery
    wal.recover();

    // check to make sure wal file is gone
    // assertTrue(new File(tmp, "import").list().length == 0);
    assertEquals(0, new File(tmp, "writing").list().length);
    assertEquals(0, new File(tmp, "sending").list().length);
    assertEquals(0, new File(tmp, "sent").list().length);
    assertEquals(0, new File(tmp, "done").list().length);
    assertEquals(1, new File(tmp, "error").list().length);
    assertEquals(1, new File(tmp, "logged").list().length);

    BenchmarkHarness.cleanupLocalWriteDir();

  }

  /**
   * This reframes data that has a bad ack-end checksum
   */
  @Test
  public void testReframeBadAckChecksum() throws IOException,
      InterruptedException {
    BenchmarkHarness.setupLocalWriteDir();
    File tmp = BenchmarkHarness.tmpdir;

    NaiveFileWALManager wal = new NaiveFileWALManager(tmp);
    wal.open(); // create dirs

    // create a seq file with no ack close.
    File f = new File(wal.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq");
    SeqfileEventSink sf = new SeqfileEventSink(f);
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(sf);
    inj.open();
    inj.append(new EventImpl("test".getBytes()));

    // need to keep the tag from the inj, but purposely mess up checksum
    Event e = inj.closeEvent();
    byte[] ref = e.get(AckChecksumInjector.ATTR_ACK_HASH);
    Arrays.fill(ref, (byte) 0);
    sf.append(e);

    // close, and do not send good ack close
    sf.close();

    // do the low level recovery
    wal.recover();

    // check to make sure wal file is gone
    // assertTrue(new File(tmp, "import").list().length == 0);
    assertEquals(0, new File(tmp, "writing").list().length);
    assertEquals(0, new File(tmp, "sending").list().length);
    assertEquals(0, new File(tmp, "sent").list().length);
    assertEquals(0, new File(tmp, "done").list().length);
    assertEquals(1, new File(tmp, "error").list().length);
    assertEquals(1, new File(tmp, "logged").list().length);

    BenchmarkHarness.cleanupLocalWriteDir();

  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */
  public void doTestBadOpen(String conflict) throws IOException {
    File src = new File(WAL_OK);
    File tmpdir = FileUtil.mktempdir();

    FileUtil.dumbfilecopy(src, new File(tmpdir, conflict));

    NaiveFileWALManager wal = new NaiveFileWALManager(tmpdir);

    try {
      wal.open();
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      FileUtil.rmr(tmpdir);
    }
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenImport() throws IOException {
    doTestBadOpen(NaiveFileWALManager.IMPORTDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenWriting() throws IOException {
    doTestBadOpen(NaiveFileWALManager.WRITINGDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenLogged() throws IOException {
    doTestBadOpen(NaiveFileWALManager.LOGGEDDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenSending() throws IOException {
    doTestBadOpen(NaiveFileWALManager.SENDINGDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenSent() throws IOException {
    doTestBadOpen(NaiveFileWALManager.SENTDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenError() throws IOException {
    doTestBadOpen(NaiveFileWALManager.ERRORDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenDone() throws IOException {
    doTestBadOpen(NaiveFileWALManager.DONEDIR);
  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */
  public void doTestBadRecover(String conflict) throws IOException {
    File tmpdir = FileUtil.mktempdir();

    File dir = new File(new File(tmpdir, conflict), "foo");
    dir.mkdirs();

    NaiveFileWALManager wal = new NaiveFileWALManager(tmpdir);

    wal.open();
    try {
      wal.recover();
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      FileUtil.rmr(tmpdir);
    }
  }

  @Test(expected = IOException.class)
  public void testBadRecoverSent() throws IOException {
    doTestBadRecover(NaiveFileWALManager.SENTDIR);
  }

  @Test(expected = IOException.class)
  public void testBadRecoverSending() throws IOException {
    doTestBadRecover(NaiveFileWALManager.SENDINGDIR);
  }

  @Test(expected = IOException.class)
  public void testBadRecoverWriting() throws IOException {
    doTestBadRecover(NaiveFileWALManager.WRITINGDIR);
  }
}
