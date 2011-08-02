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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.CompositeSink;
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

  /**
   * Create a file and write to it.
   */
  @Test(timeout = 5000)
  public void testTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    File f = File.createTempFile("temp", ".tmp");
    f.deleteOnExit();
    final CompositeSink snk = new CompositeSink(new Context(),
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
    final CompositeSink snk = new CompositeSink(new Context(),
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
    final CompositeSink snk = new CompositeSink(new Context(),
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

    final CompositeSink snk = new CompositeSink(new Context(),
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
}
