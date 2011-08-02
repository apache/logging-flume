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
package com.cloudera.flume.agent.diskfailover;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

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
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.BenchmarkHarness;
import com.cloudera.util.FileUtil;

/**
 * This tests the disk failover manager. Values append to it should be written
 * to disk and then eventually resent. AFter data reaches the SENT state, it can
 * be deleted.
 */
public class TestDiskFailoverManager {

  static Logger LOG = Logger
      .getLogger(TestDiskFailoverManager.class.getName());
  // has 5 good entries.
  final static String WAL_OK = "src/data/hadoop_logs_5.hdfs";

  @Before
  public void setUp() {
    LOG.setLevel(Level.DEBUG);
    LOG.info("====================================================");
  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */
  @Test
  public void testImport() throws IOException {

    File dir = FileUtil.mktempdir();
    // putting in large ridiculous constant
    NaiveFileFailoverManager wal = new NaiveFileFailoverManager(dir);
    wal.open();

    File logdir = new File(dir, NaiveFileFailoverManager.IMPORTDIR);
    File src = new File(WAL_OK);
    File dest = new File(logdir, "ok.0000000.20091104-101213997-0800.seq");
    FileUtil.dumbfilecopy(src, dest);
    dest.deleteOnExit();

    assertEquals(0, wal.getWritingTags().size());
    assertEquals(0, wal.getLoggedTags().size());
    assertEquals(0, wal.getSendingTags().size());

    wal.importData();

    assertEquals(0, wal.getWritingTags().size());
    assertEquals(1, wal.getLoggedTags().size());
    assertEquals(0, wal.getSendingTags().size());

    wal.close();

  }

  @Test
  public void testTransitions() throws IOException, InterruptedException {
    File dir = FileUtil.mktempdir();
    // putting in large ridiculous constant
    NaiveFileFailoverManager wal = new NaiveFileFailoverManager(dir);
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
    EventSource curSource = wal.getUnsentSource();
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
    NaiveFileFailoverManager dfo = new NaiveFileFailoverManager(tmp);
    dfo.open(); // create dirs

    File acked = new File(WAL_OK);

    // copy files and then recover them.
    FileUtil.dumbfilecopy(acked, new File(dfo.writingDir,
        "writing.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(dfo.loggedDir,
        "logged.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(dfo.sendingDir,
        "sending.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(dfo.importDir,
        "import.00000000.20100204-015814F430-0800.seq"));
    FileUtil.dumbfilecopy(acked, new File(dfo.errorDir,
        "error.00000000.20100204-015814F430-0800.seq"));
    dfo.recover();

    // check to make sure wal file is gone
    assertEquals(1,
        new File(tmp, NaiveFileFailoverManager.ERRORDIR).list().length);
    assertEquals(1,
        new File(tmp, NaiveFileFailoverManager.IMPORTDIR).list().length);
    assertEquals(3,
        new File(tmp, NaiveFileFailoverManager.LOGGEDDIR).list().length);
    assertEquals(0,
        new File(tmp, NaiveFileFailoverManager.SENDINGDIR).list().length);
    assertEquals(0,
        new File(tmp, NaiveFileFailoverManager.WRITINGDIR).list().length);

    BenchmarkHarness.cleanupLocalWriteDir();
  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */

  public void doTestBadOpen(String conflict) throws IOException {
    File src = new File(WAL_OK);
    File tmpdir = FileUtil.mktempdir();

    FileUtil.dumbfilecopy(src, new File(tmpdir, conflict));

    // putting in large ridiculous constant
    DiskFailoverManager wal = new NaiveFileFailoverManager(tmpdir);

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
    doTestBadOpen(NaiveFileFailoverManager.IMPORTDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenWriting() throws IOException {
    doTestBadOpen(NaiveFileFailoverManager.WRITINGDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenLogged() throws IOException {
    doTestBadOpen(NaiveFileFailoverManager.LOGGEDDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenSending() throws IOException {
    doTestBadOpen(NaiveFileFailoverManager.SENDINGDIR);
  }

  /**
   * Tests import to make sure we handle errors with dir problems
   */
  @Test(expected = IOException.class)
  public void testBadOpenError() throws IOException {
    doTestBadOpen(NaiveFileFailoverManager.ERRORDIR);
  }

  /**
   * Tests import to make sure it gets into the logged state properly.
   */
  public void doTestBadRecover(String conflict) throws IOException {
    File tmpdir = FileUtil.mktempdir();

    File dir = new File(new File(tmpdir, conflict), "foo");
    dir.mkdirs();

    // putting in large ridiculous constant
    NaiveFileFailoverManager wal = new NaiveFileFailoverManager(tmpdir);

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
  public void testBadRecoverSending() throws IOException {
    doTestBadRecover(NaiveFileFailoverManager.SENDINGDIR);
  }

  @Test(expected = IOException.class)
  public void testBadRecoverWriting() throws IOException {
    doTestBadRecover(NaiveFileFailoverManager.WRITINGDIR);
  }

}
