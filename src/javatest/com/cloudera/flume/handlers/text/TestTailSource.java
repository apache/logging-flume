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

package com.cloudera.flume.handlers.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.text.TailSource.Cursor;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;

/**
 * This tests the functionality of tail source using the EventSource api.
 */
public class TestTailSource {
  public static Logger LOG = Logger.getLogger(TestTailSource.class);

  @Before
  public void setDebug() {
    Logger.getLogger(TestTailSource.class).setLevel(Level.DEBUG);
    Logger.getLogger(TailSource.class).setLevel(Level.DEBUG);
  }

  @Test
  public void testTailPermissionDenied() throws IOException, FlumeSpecException, InterruptedException {
    File f;
    final EventSource eventSource;
    final CompositeSink eventSink;
    final AtomicBoolean workerFailed;
    Thread workerThread;
    FileWriter writer;
    long sleepTime;
    long eventCount;

    f = File.createTempFile("temp", ".tmp");
    f.setReadable(false, false);

    f.deleteOnExit();

    eventSource = TailSource.builder().build(f.getAbsolutePath());
    eventSink = new CompositeSink(
      new ReportTestingContext(),
      "{ delay(50) => counter(\"count\") }"
    );
    workerFailed = new AtomicBoolean(false);
    workerThread = new Thread() {

      @Override
      public void run() {
        try {
          eventSource.open();
          eventSink.open();

          EventUtil.dumpN(10, eventSource, eventSink);

          eventSource.close();
          eventSink.close();
        } catch (IOException e) {
          workerFailed.set(true);
          LOG.error("Test thread raised IOException during testing. Exception follows.", e);
        }
      }

    };

    workerThread.start();

    writer = new FileWriter(f);

    for (int i = 0; i < 10; i++) {
      writer.append("Line " + i + "\n");
      writer.flush();
    }

    writer.close();

    sleepTime = Math.round(Math.random() * 1000);

    eventCount = ((CounterSink)ReportManager.get().getReportable("count")).getCount();
    assertEquals(0, eventCount);

    LOG.debug("About to sleep for " + sleepTime + " before fixing permissions");
    Thread.sleep(sleepTime);

    f.setReadable(true, false);

    LOG.debug("Permissions fixed. Waiting for eventSource to figure it out");
    workerThread.join();

    assertFalse("Worker thread failed", workerFailed.get());

    eventCount = ((CounterSink)ReportManager.get().getReportable("count")).getCount();
    assertEquals(10, eventCount);
  }

  /**
   * Create a file and write to it.
   */
  @Test(timeout = 5000)
  public void testTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.builder().build(f.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 30;
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          snk.open();
          EventUtil.dumpN(count, src, snk);
          src.close();
          snk.close();
          done.countDown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();

    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    done.await();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());
  }

  /**
   * Create a file and write to it, move it, write another
   */
  @Test
  public void testTailSourceMove() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    File f2 = File.createTempFile("moved", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.builder().build(f.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 30;
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          snk.open();
          EventUtil.dumpN(count, src, snk);
          src.close();
          snk.close();
          done.countDown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();

    // Need to make sure the first file shows up
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count / 2; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    // This sleep is necessary to make sure the file is not moved before tail
    // sees it.
    Clock.sleep(2000);
    f.renameTo(f2);

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    LOG.info("counted " + ctr.getCount());

    FileWriter fw2 = new FileWriter(f);
    for (int i = count / 2; i < count; i++) {
      fw2.append("Line " + i + "\n");
      fw2.flush();
    }
    fw2.close();

    done.await();

    ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());
  }

  /**
   * Create and tail multiple files
   */
  @Test(timeout = 15000)
  public void testMultiTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    File f2 = File.createTempFile("temp", ".tmp");
    f2.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = TailSource.multiTailBuilder().build(
        f.getAbsolutePath(), f2.getAbsolutePath());
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 60;
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          snk.open();
          EventUtil.dumpN(count, src, snk);
          src.close();
          snk.close();
          done.countDown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();

    int log1 = 0, log2 = 0;
    FileWriter fw = new FileWriter(f);
    FileWriter fw2 = new FileWriter(f2);
    for (int i = 0; i < count; i++) {
      if (Math.random() > 0.5) {
        fw.append("Line " + i + "\n");
        fw.flush();
        log1++;
      } else {
        fw2.append("Line " + i + "\n");
        fw2.flush();
        log2++;
      }
    }
    fw.close();
    fw2.close();

    done.await();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    LOG.info("events in file1: " + log1 + " events in file2: " + log2);
    assertEquals(count, ctr.getCount());

  }

  /**
   * This is a tail source that starts from the end of file.
   */
  @Test
  public void testTailSourceStartFromEnd() throws IOException,
      FlumeSpecException, InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();

    // pre-existing file
    final int count = 30;
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.append("Line " + i + "\n");
      fw.flush();
    }
    fw.close();

    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    // Test start from end.
    final TailSource src = (TailSource) TailSource.builder().build(
        f.getAbsolutePath(), "true");
    final CountDownLatch done = new CountDownLatch(1);

    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          snk.open();
          EventUtil.dumpN(count, src, snk);
          src.close();
          snk.close();
          done.countDown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();

    FileWriter fw2 = new FileWriter(f, true);
    for (int i = 0; i < count; i++) {
      fw2.append("Line " + i + "\n");
      fw2.flush();
    }
    fw2.close();

    done.await();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(count, ctr.getCount());

    Cursor cursor = src.cursors.get(0);
    assertEquals(cursor.lastFileLen, cursor.lastReadOffset);
  }

  /**
   * Regression test for FLUME-218: Ensure cursor is not reset to the beginning
   * of a file if the event rate exceeds a certain level or delays are
   * introduced.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   */
  @Test
  public void testResetRaceCondition() throws IOException {
    File tmpFile;
    final EventSource source;
    final EventSink sink;
    final AtomicBoolean workerFailed;
    FileOutputStream os;
    Thread thread;

    tmpFile = File.createTempFile("tmp-", ".tmp");
    tmpFile.deleteOnExit();

    source = TailSource.builder().build(tmpFile.getAbsolutePath(), "true");
    sink = CounterSink.builder().build(new ReportTestingContext(), "count");
    workerFailed = new AtomicBoolean(false);
    os = null;

    /*
     * A worker thread that blindly moves events until we send a poison pill
     * message containing "EOF".
     */
    thread = new Thread() {

      @Override
      public void run() {
        try {
          Event e;

          source.open();
          sink.open();

          e = null;

          do {
            e = source.next();
            sink.append(e);
          } while (e != null && !Arrays.equals(e.getBody(), "EOF".getBytes()));

          source.close();
          sink.close();
        } catch (IOException e) {
          LOG.error("Error while reading from / write to flume source / sink. Exception follows.", e);
          workerFailed.set(true);
        }
      }
    };

    thread.start();

    /*
     * Throw 1000 filler events into our tmp file (purposefully unbuffered) and
     * ensure we get fewer than 50 duplicates.
     */
    try {
      os = new FileOutputStream(tmpFile);

      for (int i = 0; i < 1000; i++) {

        if (i % 100 == 0) {
          os.flush();
          os.close();
          os = new FileOutputStream(tmpFile);
        }

        os.write((i + " 12345678901234567890123456789012345678901234567890123456789012334567890\n").getBytes());
        Clock.sleep(20);
      }

      os.write("EOF".getBytes());
      os.flush();
    } catch (IOException e) {
      LOG.error("Error while writing to tmp tail source file. Exception follows.", e);
      Assert.fail();
    } catch (InterruptedException e) {
      LOG.error("Error while writing to tmp tail source file. Interrupted during a sleep. Exception follows.", e);
      Assert.fail();
    } finally {
      if (os != null) {
        os.close();
      }
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      Assert.fail("Failed to wait for worker thread to complete - interrupted");
    }

    Assert.assertFalse("Worker thread failed. Check logs for errors.", workerFailed.get());

    /*
     * FIXME - These tests should be uncommented when TailSource no longer
     * sleep()s. Currently, this causes a race condition where a file being
     * written to and truncated during a sleep causes a loss of data.
     * 
     * Assert.assertTrue("Saw fewer than 1000 events.", ((CounterSink)
     * sink).getCount() > 1000);
     * Assert.assertTrue("Saw more than 50 dupes for 1000 events",
     * (((CounterSink) sink).getCount() - 1000) < 50);
     */
  }

}
