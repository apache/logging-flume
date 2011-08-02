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
package com.cloudera.flume.handlers.syslog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.debug.NoNlSynthSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.util.Benchmark;

/**
 * This demonstrates the rate that these different extractors work at.
 * 
 * test on the same machine"
 * 
 * Old method using regex: 1M messages, 100 bytes each, 8.4s => 11,9 MB/s (which
 * is close to SyslogTcp socket throughput limit). (Apparently there were bugs
 * in the regex)
 * 
 * 500MB / 41.0 s => `12.2 MB/s
 * 
 * New method using custom parser removing time syscalls: 1M message, 100 bytes,
 * each, 5x
 * 
 * 500MB / 15.3 => 32.7 MB/s
 */
public class PerfSyslogWireExtract {

  /**
   * Generates a dataset, puts it into a memory buffer, and the uses the
   * DataInputStream machinery to read through it 100 bytes at a time.
   * 
   * 1M x 100 bytes, 5 times
   */
  @Test
  public void testNewExtractScan100() throws IOException, EventExtractException {
    Benchmark b = new Benchmark("new extract - scan 100 blocks");

    b.mark("build dataset");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // 1M x 100 byte messages, 0 is the rand seed
    NoNlSynthSource src = new NoNlSynthSource(1000000, 100, 0);

    src.open();
    Event e = null;
    while ((e = src.next()) != null) {
      out.write("<33>".getBytes());
      out.write(e.getBody());
      out.write('\n');
    }

    b.mark("start parsing dataset");
    int good = 0;
    int bad = 0;
    int lines = 0;

    // We do this test 100 times!
    for (int i = 0; i < 5; i++) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(out
          .toByteArray()));
      lines++;
      try {
        byte[] data = new byte[100];
        while (true)
          in.readFully(data);
      } catch (EOFException eof) {
        // expected.
      }

    }
    b.mark("complete-good-bad", good, bad, lines);
    b.done();
  }

  /**
   * Generates a dataset, puts it into a memory buffer, and the uses the
   * DataInputStream machinery to read through it 1000 bytes at a time.
   * 
   * 1M x 100 bytes, 5 times
   */
  @Test
  public void testNewExtractScan1000() throws IOException,
      EventExtractException {
    Benchmark b = new Benchmark("new extract - scan 1000 blocks");

    b.mark("build dataset");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // 1M x 100 byte messages, 0 is the rand seed
    NoNlSynthSource src = new NoNlSynthSource(1000000, 100, 0);

    src.open();
    Event e = null;
    while ((e = src.next()) != null) {
      out.write("<33>".getBytes());
      out.write(e.getBody());
      out.write('\n');
    }

    b.mark("start parsing dataset");
    int good = 0;
    int bad = 0;
    int lines = 0;

    // We do this test 100 times!
    for (int i = 0; i < 5; i++) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(out
          .toByteArray()));
      try {
        byte[] data = new byte[1000];
        while (true) {
          lines++;

          in.readFully(data);
        }
      } catch (EOFException eof) {
        // expected.
      }

    }
    b.mark("complete-good-bad", good, bad, lines);
    b.done();
  }

  /**
   * Generates a dataset, puts it into a memory buffer, and the uses the
   * DataInputStream machinery to read through it one byte at a time.
   * 
   * 1M x 100 bytes, 5 times
   */
  @Test
  public void testNewExtractScan() throws IOException, EventExtractException {
    Benchmark b = new Benchmark("new extract - scan single byte");

    b.mark("build dataset");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // 1M x 100 byte messages, 0 is the rand seed
    NoNlSynthSource src = new NoNlSynthSource(1000000, 100, 0);

    src.open();
    Event e = null;
    while ((e = src.next()) != null) {
      out.write("<33>".getBytes());
      out.write(e.getBody());
      out.write('\n');
    }

    b.mark("start parsing dataset");
    int good = 0;
    int bad = 0;
    int lines = 0;

    // We do this test 100 times!
    for (int i = 0; i < 5; i++) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(out
          .toByteArray()));
      try {
        while (true) {
          lines++;

          in.readByte();
        }
      } catch (EOFException eof) {
        // expected.
      }

    }
    b.mark("complete-good-bad", good, bad, lines);
    b.done();
  }

  /**
   * Generates a dataset, puts it into a memory buffer, and the uses the
   * DataInputStream machinery to read through it one parsed record at a time.
   */
  @Test
  public void testNewExtract() throws IOException, EventExtractException {
    Benchmark b = new Benchmark("regex extract");

    b.mark("build dataset");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // 1M x 100 byte messages, 0 is the rand seed
    NoNlSynthSource src = new NoNlSynthSource(1000000, 100, 0);

    src.open();
    Event e = null;
    while ((e = src.next()) != null) {
      out.write("<33>".getBytes());
      out.write(e.getBody());
      out.write('\n');
    }

    byte[] outbytes = out.toByteArray();
    System.out.println("Outbytes length : " + outbytes.length);
    b.mark("start parsing dataset");
    int good = 0;
    int bad = 0;
    int lines = 0;

    // We do this test 50 times!
    for (int i = 0; i < 5; i++) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(
          outbytes));

      Event evt = null;
      while (true) {
        try {
          lines++;
          evt = SyslogWireExtractor.extractEvent(in);
          if (evt == null)
            break;
          good++;
        } catch (Exception eee) {
          bad++;
        }
      }
    }
    b.mark("complete-good-bad", good, bad, lines);
    b.done();
  }

  /**
   * A harness so that the profiler can attach to the process.
   */
  static public void main(String[] argv) throws IOException,
      EventExtractException {
    // A harness for the profiler.

    new PerfSyslogWireExtract().testNewExtract();
    // new PerfNewSyslogWireExtract().testOldExtract();
    // new PerfNewSyslogWireExtract().testNewBlockExtract();
  }

}
