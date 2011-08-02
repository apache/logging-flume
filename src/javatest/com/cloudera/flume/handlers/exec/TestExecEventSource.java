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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.FileUtil;

import org.junit.Test;

/**
 * Tests for the shell-based process exec event source.
 */
public class TestExecEventSource {
  static Logger LOG = Logger.getLogger(TestExecEventSource.class.getName());

  /**
   * Test builders
   */
  @Test
  public void testStreamBuilder() throws IOException, FlumeSpecException {
    ExecEventSource e = (ExecEventSource) FlumeBuilder
        .buildSource("execStream(\"ps -aux\")");
    assertEquals(e.aggregate, false);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, false);
    assertEquals(e.period, 0);
  }

  @Test
  public void testExecBuilder() throws IOException, FlumeSpecException {
    ExecEventSource e = (ExecEventSource) FlumeBuilder
        .buildSource("exec(\"ps -aux\", true, true, 1000)");
    assertEquals(e.aggregate, true);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, true);
    assertEquals(e.period, 1000);
  }

  @Test
  public void testPeriodicBuilder() throws IOException, FlumeSpecException {
    ExecEventSource e = (ExecEventSource) FlumeBuilder
        .buildSource("execPeriodic(\"ps -aux\", 1000)");
    assertEquals(e.aggregate, true);
    assertEquals(e.command, "ps -aux");
    assertEquals(e.restart, true);
    assertEquals(e.period, 1000);
  }

  /**
   * Test that a too-large event is caught and source is closed cleanly
   */
  @Test
  public void testLargeEvent() throws IOException, FlumeSpecException {
    int max = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    File tmpdir = FileUtil.mktempdir();
    File tmp = new File(tmpdir, "flume-tmp");
    StringBuilder tooLarge = new StringBuilder();
    for (int i = 0; i < max + 5; ++i) {
      tooLarge.append("X");
    }    
    FileOutputStream f = new FileOutputStream(tmp);
    f.write(tooLarge.toString().getBytes());
    f.close();
    EventSource source = new ExecEventSource.Builder().build("cat "
        + tmp.getCanonicalPath());
    source.open();
    assertNull(source.next());    
    source.close();    
    FileUtil.rmr(tmpdir);
    // Check that the stdout reader closed correctly
    assertTrue(((ExecEventSource)source).readOut.signalDone.get()); 
  }

  /**
   * Test process creation and reading from stdout; assumes 'yes' is available.
   */
  public void testSimpleExec() throws IOException {
    try {
      EventSource source = new ExecEventSource.Builder().build("yes");
      source.open();
      Event e = source.next();
      String body = new String(e.getBody());
      assertEquals("Expected event body to be 'y', but got " + body, body, "y");
      source.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Test that an empty command throws an exception
   */
  @Test
  public void testEmptyCommand() throws IOException {
    Exception e = null;
    try {
      EventSource source = new ExecEventSource.Builder().build("");
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

    EventSource source = new ExecEventSource.Builder()
        .build("/bin/bash " + cmd);
    source.open();
    Event e = source.next();
    String body = new String(e.getBody());
    String src = new String(e.get(ExecEventSource.A_PROC_SOURCE));
    source.close();
    assertEquals("Expected event body to be 'Hello World!', but got " + body,
        body, "Hello world!");
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

    final EventSource source = new ExecEventSource.Builder().build("/bin/bash "
        + cmd);
    final CountDownLatch latch = new CountDownLatch(1);
    source.open();
    Thread t = new Thread() {
      public void run() {
        try {
          source.next();
          latch.countDown();
        } catch (Exception e) {
          LOG.warn("Event consumption thread caught exception, test will fail",
              e);
        }
      }
    };
    t.start();
    source.close();
    assertTrue("source.next did not exit after close within 5 seconds", latch
        .await(5000, TimeUnit.MILLISECONDS));
  }

  /**
   * Test that aggregation works correctly - wait until file handle is closed
   * before returning the entire output
   */
  @Test
  public void testAggregate() throws IOException {
    File temp = File.createTempFile("flmtst", null);
    temp.deleteOnExit();

    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
    out.write("#!/bin/bash\n echo \"Hello\"\necho \"World!\"\n");
    out.close();
    String cmd = temp.getAbsolutePath();
    ExecEventSource.Builder builder = new ExecEventSource.Builder();
    EventSource source = builder.build("/bin/bash " + cmd, "true");
    source.open();
    Event e = source.next();
    source.close();
    String output = new String(e.getBody());
    assertEquals(
        "Expected aggregate exec output to be 'Hello\nWorld!', but got "
            + output, output, "Hello\nWorld!\n");

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
    ExecEventSource.Builder builder = new ExecEventSource.Builder();
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
    ExecEventSource.Builder builder = new ExecEventSource.Builder();
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
}
