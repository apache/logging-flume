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
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * This class tests the tail dir source. It looks at a local file system and
 * tails all the files in the directory. If new files appear in the directory it
 * tails them as well.
 */

public class TestTailDirSource {

  @Test
  public void testOpenClose() throws IOException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    for (int i = 0; i < 20; i++) {
      src.open();
      src.close();
    }
    FileUtil.rmr(tmpdir);
  }

  @Test
  public void testBuilder() throws IOException, FlumeSpecException {
    File tmpdir = FileUtil.mktempdir();
    String src = "tailDir(\"" + tmpdir.getAbsolutePath() + "\", \"foo.*\")";
    FlumeBuilder.buildSource(src);
    FileUtil.rmr(tmpdir);
  }

  @Test(expected = FlumeSpecException.class)
  public void testFailBuilder() throws IOException, FlumeSpecException {
    File tmpdir = FileUtil.mktempdir();
    String src = "tailDir(\"" + tmpdir.getAbsolutePath() + "\", \"\\x.*\")";
    FlumeBuilder.buildSource(src);
    FileUtil.rmr(tmpdir);
  }

  void genFiles(File tmpdir, String prefix, int files, int lines)
      throws IOException {
    for (int i = 0; i < files; i++) {
      File tmpfile = File.createTempFile(prefix, "bar", tmpdir);
      PrintWriter pw = new PrintWriter(tmpfile);
      for (int j = 0; j < lines; j++) {
        pw.println("this is file " + i + " line " + j);
      }
      pw.close();
    }
  }

  /**
   * Setup sources and sinks, start the driver, then have new files appear,
   */
  @Test
  public void testNoneToNewFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);

    genFiles(tmpdir, "foo", 10, 100);

    Clock.sleep(1000);

    assertEquals(1000, cnt.getCount());

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);
  }

  /**
   * Setup sources and sinks, start the driver, then have new files appear. The
   * filter should only pick up the foo* files and not the bar* files
   */
  @Test
  public void testNoneToNewFilteredFiles() throws IOException,
      InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, "foo.*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();
    Clock.sleep(1000);

    genFiles(tmpdir, "foo", 10, 100);
    genFiles(tmpdir, "bar", 15, 100);

    Clock.sleep(1000);

    assertEquals(1000, cnt.getCount());

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);
  }

  @Test
  public void testExistingFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);

  }

  /**
   * Start with existing files, check that the are read, wait a little bit and
   * then add some new files and make sure they are read.
   */
  @Test
  public void testExistingAddFiles() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    // more files show up
    genFiles(tmpdir, "foo", 10, 100);
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);

  }

  @Test
  public void testExistingRemoveFiles() throws IOException,
      InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    genFiles(tmpdir, "foo", 10, 100);

    drv.start();
    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    FileUtil.rmr(tmpdir);

    Clock.sleep(1000);
    assertEquals(1000, cnt.getCount());

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);
  }

  /**
   * This creates a many files that need to show up and then get deleted. This
   * just verifies that the files have been removed.
   */
  @Test
  public void testCursorExhaustion() throws IOException, InterruptedException {
    File tmpdir = FileUtil.mktempdir();
    TailDirSource src = new TailDirSource(tmpdir, ".*");
    AccumulatorSink cnt = new AccumulatorSink("tailcount");
    src.open();
    cnt.open();
    DirectDriver drv = new DirectDriver(src, cnt);

    drv.start();

    // This blows up when there are ~2000 files
    genFiles(tmpdir, "foo", 200, 10);
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    ReportEvent rpt1 = src.getReport();
    assertEquals(Long.valueOf(200), rpt1
        .getLongMetric(TailDirSource.A_FILESPRESENT));

    FileUtil.rmr(tmpdir);
    tmpdir.mkdirs();
    Clock.sleep(1000);
    assertEquals(2000, cnt.getCount());

    ReportEvent rpt = src.getReport();
    assertEquals(rpt.getLongMetric(TailDirSource.A_FILESADDED), rpt
        .getLongMetric(TailDirSource.A_FILESDELETED));
    assertEquals(Long.valueOf(0), rpt
        .getLongMetric(TailDirSource.A_FILESPRESENT));

    drv.stop();
    src.close();
    cnt.close();
    FileUtil.rmr(tmpdir);
  }
}
