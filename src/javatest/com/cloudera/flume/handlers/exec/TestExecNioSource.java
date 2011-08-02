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
package com.cloudera.flume.handlers.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * Tests for the shell-based process exec event source.
 */
public class TestExecNioSource {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestExecNioSource.class);

  /**
   * Test builders
   */
  @Test
  public void testStreamBuilder() throws IOException, FlumeSpecException {
    ExecNioSource e = (ExecNioSource) ExecNioSource.buildStream().build(
        "ps -aux");
    assertEquals(e.inAggregateMode, false);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, false);
    assertEquals(e.period, 0);
  }

  @Test
  public void testExecBuilder() throws IOException, FlumeSpecException {
    ExecNioSource e = (ExecNioSource) ExecNioSource.builder().build("ps -aux",
        "true", "true", "1000");
    assertEquals(e.inAggregateMode, true);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, true);
    assertEquals(e.period, 1000);
  }

  @Test
  public void testPeriodicBuilder() throws IOException, FlumeSpecException {
    ExecNioSource e = (ExecNioSource) ExecNioSource.buildPeriodic().build(
        "ps -aux", "1000");
    assertEquals(e.inAggregateMode, true);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, true);
    assertEquals(e.period, 1000);
  }

  /**
   * Test the extract lines function.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testExtractLines() throws InterruptedException {
    LinkedBlockingQueue<Event> q = new LinkedBlockingQueue<Event>();
    long maxSz = FlumeConfiguration.get().getEventMaxSizeBytes();
    ByteBuffer buf = ByteBuffer.allocate((int) maxSz * 4);

    // Empty Case
    boolean ret = ExecNioSource.extractLines(buf, "cmd", "tag", q);
    assertFalse(ret);
    assertEquals(0, q.size());

    // Overflow Case
    // this should fill the buffer and but progress should be made.
    for (int i = 0; i < maxSz; i++) {
      buf.put((byte) 'a');
    }

    ret = ExecNioSource.extractLines(buf, "cmd", "tag", q);
    assertFalse(ret);
    assertEquals(maxSz, buf.position()); // should have buffer cleared
    assertEquals(0, q.size());

    buf.put((byte) '\n');
    ret = ExecNioSource.extractLines(buf, "cmd", "tag", q);
    assertTrue(ret);
    assertEquals(0, buf.position()); // should have buffer cleared
    assertEquals(1, q.size());
    Event e = q.poll();
    assertEquals(maxSz, e.getBody().length);

    // Normal Case
    for (int i = 0; i < maxSz; i++) {
      if (i % 1024 == 1023) {
        buf.put((byte) '\n');
      } else {
        buf.put((byte) 'b');
      }
    }
    ret = ExecNioSource.extractLines(buf, "cmd", "tag", q);
    assertTrue(ret);
    assertEquals(0, buf.position()); // should have buffer cleared
    assertEquals(32, q.size());
    for (int i = 0; i < 32; i++) {
      e = q.poll();
      assertEquals(1023, e.getBody().length);
    }
  }

  /**
   * Test the dropUntilNewLineFunction
   */
  @Test
  public void testDropUntilNewLine() {
    long maxSz = FlumeConfiguration.get().getEventMaxSizeBytes();
    ByteBuffer buf = ByteBuffer.allocate((int) maxSz * 4);

    // Empty Case
    boolean ret = ExecNioSource.dropUntilNewLine(buf);
    assertTrue(ret); // not new line yet
    assertEquals(0, buf.position());

    // Overflow Case
    // this should fill the buffer and but progress should be made.
    for (int i = 0; i < maxSz; i++) {
      buf.put((byte) 'a');
    }

    ret = ExecNioSource.dropUntilNewLine(buf);
    assertTrue(ret);
    assertEquals(0, buf.position()); // should have buffer cleared

    buf.put((byte) '\n');
    buf.put((byte) 'd');
    ret = ExecNioSource.dropUntilNewLine(buf);
    assertFalse(ret);
    assertEquals(1, buf.position()); // should have buffer cleared

    buf.clear();

    // Normal Case
    for (int i = 0; i < maxSz; i++) {
      if (i % 1024 == 1023) {
        buf.put((byte) '\n');
      } else {
        buf.put((byte) 'b');
      }
    }
    ret = ExecNioSource.dropUntilNewLine(buf);
    assertFalse(ret);
    // cleared until first '\n' then and leaves remaining data
    assertEquals(maxSz - 1024, buf.position());

  }

  /**
   * Test that a too-large event is truncated, warns, and continues happily
   * 
   * @throws FlumeSpecException
   * @throws IOException
   */
  @Test
  public void testLineModeLargeEvents() throws IOException, FlumeSpecException {
    testLineModeLargeEventOverflow(0);
    testLineModeLargeEventOverflow(1);
    testLineModeLargeEventOverflow(5);
  }

  public void testLineModeLargeEventOverflow(int overflow) throws IOException,
      FlumeSpecException {
    int max = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    File tmpdir = FileUtil.mktempdir();
    File tmp = new File(tmpdir, "flume-tmp");
    StringBuilder tooLarge = new StringBuilder();
    for (int i = 0; i < max + overflow; ++i) {
      tooLarge.append("X");
    }
    tooLarge.append("\n");

    FileOutputStream f = new FileOutputStream(tmp);
    f.write(tooLarge.toString().getBytes());
    f.close();
    EventSource source = new ExecNioSource.Builder().build("cat "
        + tmp.getCanonicalPath());
    source.open();
    Event e = source.next();
    assertNotNull(e);
    assertEquals(max, e.getBody().length); // check that event was truncated.
    source.close();
    FileUtil.rmr(tmpdir);
    // Check that the stdout reader closed correctly
    assertTrue(((ExecNioSource) source).readOut.signalDone.get());
  }

  /**
   * Test that a too-large event is truncated, warns, and continues happily
   * 
   * @throws FlumeSpecException
   * @throws IOException
   */
  @Test
  public void testAggModeLargeEventOverflow() throws IOException,
      FlumeSpecException {
    int overflow = 5;
    int max = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    File tmpdir = FileUtil.mktempdir();
    File tmp = new File(tmpdir, "flume-tmp");
    StringBuilder tooLarge = new StringBuilder();
    for (int i = 0; i < max + overflow; ++i) {
      tooLarge.append("X");
    }
    tooLarge.append("\n");

    FileOutputStream f = new FileOutputStream(tmp);
    f.write(tooLarge.toString().getBytes());
    f.close();
    EventSource source = new ExecNioSource.Builder().build("cat "
        + tmp.getCanonicalPath(), "true");
    source.open();
    Event e = source.next();
    assertNotNull(e);
    assertEquals(max, e.getBody().length); // check that event was truncated.
    source.close();
    FileUtil.rmr(tmpdir);
    // Check that the stdout reader closed correctly
    assertTrue(((ExecNioSource) source).readOut.signalDone.get());
  }

  /**
   * Test that an event that gets written slowly gets fully read.
   * 
   * @throws FlumeSpecException
   * @throws IOException
   */
  @Test
  public void testAggModeMultiWrite() throws IOException, FlumeSpecException {

    File temp = File.createTempFile("slowwrite", ".sh");
    temp.deleteOnExit();
    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    // echo, sleep 1s, echo
    out.write("#!/bin/bash\necho \"Hello world!\" >&2 \n"
        + "sleep 1s \necho \"Hello world!\" >&2 \n");
    out.close();
    String cmd = temp.getAbsolutePath();

    EventSource source = new ExecNioSource.Builder().build("/bin/bash " + cmd,
        "true");

    source.open();
    Event e = source.next();
    assertNotNull(e);
    assertEquals(26, e.getBody().length); // check that we read both lines
    source.close();

    // Check that the stdout reader closed correctly
    assertTrue(((ExecNioSource) source).readOut.signalDone.get());
  }

  /**
   * Test process creation and reading from stdout; assumes 'yes' is available.
   */
  public void testSimpleExec() throws IOException {
    try {
      EventSource source = new ExecNioSource.Builder().build("yes");
      source.open();
      Event e = source.next();
      String body = new String(e.getBody());
      assertEquals("Expected event body to be 'y', but got " + body, body, "y");
      source.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
  }

  /**
   * Test that an empty command throws an exception
   */
  @Test
  public void testEmptyCommand() throws IOException {
    Exception e = null;
    try {
      EventSource source = new ExecNioSource.Builder().build("");
      source.open();
      source.close();
    } catch (IllegalArgumentException i) {
      e = i;
    }
    assertNotNull("testEmptyCommand expected IllegalArgumentException", e);
  }

  /**
   * Test that we get some output from stderr
   */
  @Test
  public void testReadStdErr() throws IOException {
    File temp = File.createTempFile("flmtst", null);
    temp.deleteOnExit();

    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    out.write("#!/bin/bash\necho \"Hello world!\" >&2 \n");
    out.close();
    String cmd = temp.getAbsolutePath();

    EventSource source = new ExecNioSource.Builder().build("/bin/bash " + cmd);
    source.open();
    Event e = source.next();
    String body = new String(e.getBody());
    String src = new String(e.get(ExecNioSource.A_PROC_SOURCE));
    source.close();
    assertEquals("Expected event body to be 'Hello World!', but got '" + body
        + "'", body, "Hello world!");
    assertEquals("Expected to receive event from STDERR, but got " + src, src,
        "STDERR");
  }

  /**
   * Test that closing the source stops the reader threads
   */
  @Test
  public void testClose() throws IOException, InterruptedException {
    File temp = File.createTempFile("flmtst", null);
    temp.deleteOnExit();

    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    out.write("#!/bin/bash\n while true; do sleep 1; done; >&2 \n");
    out.close();
    String cmd = temp.getAbsolutePath();

    final EventSource source = new ExecNioSource.Builder().build("/bin/bash "
        + cmd);
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread() {
      public void run() {
        try {
          source.open();
          started.countDown();
          source.next();
          latch.countDown();
        } catch (Exception e) {
          LOG.warn("Event consumption thread caught exception, test will fail",
              e);
        }
      }
    };
    t.start();
    started.await(1000, TimeUnit.MILLISECONDS);
    Clock.sleep(100); // give next a chance to start.
    source.close();
    assertTrue("source.next did not exit after close within 5 seconds", latch
        .await(5000, TimeUnit.MILLISECONDS));
  }

  @Ignore
  @Test(timeout = 1000)
  public void testNewChannelBlockingSemantics() throws IOException {
    ReadableByteChannel ch = Channels.newChannel(System.in);

    ByteBuffer buf = ByteBuffer.allocate(10);
    int rdSz = ch.read(buf); // this operation blocks!
    assertTrue(rdSz < 0);
  }

  @Ignore
  @Test
  public void testNewChannelBlockingCloseSemantics() throws IOException,
      InterruptedException {

    final ReadableByteChannel ch = Channels.newChannel(System.in);
    final CountDownLatch done = new CountDownLatch(1);

    new Thread() {
      public void run() {
        ByteBuffer buf = ByteBuffer.allocate(10);
        try {
          ch.read(buf);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } finally {
          // this operation blocks!
          done.countDown();
        }
      }
    }.start();
    Clock.sleep(250); // get read got blocked
    ch.close();
    System.in.close();

    assertTrue("Timeout becuase channel read blocked", done.await(5000,
        TimeUnit.MILLISECONDS));
  }

  @Test
  public void testNewPipeBlockingSemantics() throws IOException {
    Pipe pipe = Pipe.open();
    ByteBuffer buf = ByteBuffer.allocate(10);
    assertTrue(pipe.source().isBlocking());
    pipe.source().configureBlocking(false);
    assertTrue(!pipe.source().isBlocking());
    int rdSz = pipe.source().read(buf); // this operation blocks!
    assertTrue(rdSz <= 0);
  }

  @Test
  public void testNewPipePartialReadSemantics() throws IOException {
    Pipe pipe = Pipe.open();
    int sz = 10;
    ByteBuffer buf = ByteBuffer.allocate(sz);
    pipe.sink().write(ByteBuffer.wrap("small".getBytes()));
    int rdSz = pipe.source().read(buf); // this operation blocks!
    assertTrue(rdSz <= sz);
  }

  /**
   * Test that aggregation works correctly - wait until file handle is closed
   * before returning the entire output
   * 
   * @throws InterruptedException
   */
  @Test
  public void testAggregate() throws IOException, InterruptedException {
    File temp = File.createTempFile("flmtst", null);
    temp.deleteOnExit();

    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    out.write("#!/bin/bash\n echo \"Hello\"\necho \"World!\"\n");
    out.close();
    String cmd = temp.getAbsolutePath();
    ExecNioSource.Builder builder = new ExecNioSource.Builder();
    EventSource source = builder.build("/bin/bash " + cmd, "true");
    source.open();

    Clock.sleep(250); // need to sleep to let things percolate through threads.
    Event e = source.next();
    source.close();
    String output = new String(e.getBody());
    assertEquals("Expected aggregate exec output to be 'Hello\nWorld!', "
        + "but got '" + output + "'", output, "Hello\nWorld!\n");

  }

  /**
   * Test that restarting an exec after a pause works correctly. Requires
   * GNU-compatible 'date' to work correctly.
   */
  @Test
  public void testRestart() throws IOException, InterruptedException {
    File temp = File.createTempFile("flmtst", null);
    temp.deleteOnExit();

    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    out.write("#!/bin/bash\n date +%s");
    out.close();
    String cmd = temp.getAbsolutePath();
    ExecNioSource.Builder builder = new ExecNioSource.Builder();
    // aggregate = false, restart=true, restart after 1000ms
    EventSource source = builder.build("/bin/bash " + cmd, "false", "true",
        "1000");
    source.open();
    Event e1 = source.next();
    Event e2 = source.next();
    source.close();
    String t1 = new String(e1.getBody());
    String t2 = new String(e2.getBody());
    long time1 = Long.parseLong(t1);
    long time2 = Long.parseLong(t2);
    assertTrue("Time difference with repeated exec should be >= 1s", time2
        - time1 >= 1);
  }

  /**
   * Test that opening and closing a large number of times works correctly. In
   * order to hit file leak problems deterministically we need to know ulimit -n
   * on the local machine, but an assumption of 1024 is workable for now. So
   * this test can give false successes, but no false failures.
   */
  @Test
  public void testRepeatedOpenClose() {
    ExecNioSource.Builder builder = new ExecNioSource.Builder();
    EventSource source = builder.build("date");
    for (int i = 0; i < 1024; ++i) {
      try {
        source = builder.build("date");
        source.open();
        source.close();
      } catch (Exception e) {
        assertTrue("Exec open / close failed on iteration " + i
            + " with failure " + e, false);
      }
    }
  }

  @Test
  public void testNoRandomPrepend() throws IOException {
    File f = File.createTempFile("prepend", "bar");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);

    ExecNioSource.Builder builder = new ExecNioSource.Builder();
    EventSource source = builder.build("tail -F " + f.getAbsolutePath());
    source.open();

    fw.write("foo\n");
    fw.flush();
    Event e = source.next();
    assertTrue(Arrays.equals("foo".getBytes(), e.getBody()));

    fw.write("bar\n");
    fw.flush();
    e = source.next();
    assertTrue(Arrays.equals("bar".getBytes(), e.getBody()));

    fw.write("baz\n");
    fw.flush();
    e = source.next();
    assertTrue(Arrays.equals("baz".getBytes(), e.getBody()));

    source.close();
  }

  @Test
  public void testTruncateLine() throws IOException {
    File f = File.createTempFile("truncate", ".bar");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);

    ExecNioSource.Builder builder = new ExecNioSource.Builder();
    EventSource source = builder.build("tail -F " + f.getAbsolutePath());
    source.open();

    fw.write("foo\n");
    fw.flush();
    Event e = source.next();
    assertTrue(Arrays.equals("foo".getBytes(), e.getBody()));

    // this case inserts an extremely long line.
    int max = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    int tooBig = max * 10;
    byte[] data = new byte[tooBig];
    for (int i = 0; i < data.length; i++) {
      data[i] = 'a';
    }
    fw.write(new String(data) + "\n");
    fw.flush();
    e = source.next();
    assertEquals(max, e.getBody().length);

    // back to previous test
    fw.write("baz\n");
    fw.flush();
    e = source.next();
    assertTrue(Arrays.equals("baz".getBytes(), e.getBody()));

    source.close();
  }

}
