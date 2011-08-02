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
package com.cloudera.util;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.google.common.base.Preconditions;

/**
 * Simple class to consolidate benchmarking and timing code. This will help me
 * find the bottlenecks.
 * 
 * Usage: create new benchmark. call mark with comments whenever wanted. call
 * done to flush print writer.
 * 
 * std out is used for human readable results, stderr is used for csv formatted
 * data.
 */
public class Benchmark {
  long start;
  long last;
  PrintWriter out;
  PrintWriter log;
  List<String> values = new ArrayList<String>();
  String name;

  public Benchmark(String name, PrintWriter o, PrintWriter l) {
    Preconditions.checkNotNull(o);
    Preconditions.checkNotNull(l);

    this.name = name;
    out = o;
    log = l;

    // do this first because of potential dns lookup delay.
    try {
      String host = InetAddress.getLocalHost().getHostName();
      values.add(host);
    } catch (UnknownHostException e) {
      values.add("localhost");
      e.printStackTrace();
    }

    start = System.nanoTime();
    last = start;
    // do a GC cleanup before each benchmark test.
    // Runtime rt = Runtime.getRuntime();
    // rt.gc();
    flushMemory();

    Runtime r = Runtime.getRuntime();
    long fmem = r.freeMemory();
    long tmem = r.totalMemory();
    long umem = tmem - fmem; // memory used
    out.printf("[%,18dus, %,18d b mem]\tStarting (after gc) \n", 0, umem);
  }

  // default to standard out.
  public Benchmark() {
    this(null, new PrintWriter(new OutputStreamWriter(System.out)),
        new PrintWriter(new OutputStreamWriter(System.err)));
  }

  // this was named, make the values go to std err.
  public Benchmark(String name) {
    this(name, new PrintWriter(new OutputStreamWriter(System.out)),
        new PrintWriter(new OutputStreamWriter(System.err)));
  }

  public void mark(Object... logs) {
    StringBuffer b = new StringBuffer();
    boolean first = true;
    for (Object o : logs) {
      if (!first)
        b.append(',');
      b.append(o.toString());
      first = false;
      values.add(o.toString());
    }

    _mark(b.toString());
  }

  void _mark(String s) {
    // record time before we incur gc
    long now = System.nanoTime();
    long delta = (now - last);
    long cumulative = (now - start);

    // force a GC to get memory usage estimate
    flushMemory();

    Runtime r = Runtime.getRuntime();
    long fmem = r.freeMemory();
    long tmem = r.totalMemory();
    long umem = tmem - fmem; // memory used

    // results
    out.printf("[%,18dns d %,18dns %,18d b mem]\t%s\n", cumulative, delta,
        umem, s);
    // values.add(s);
    values.add("" + delta);
    values.add("" + umem);

    // skip over gc time
    last = System.nanoTime(); // don't count gc time.
  }

  /**
   * Dumps results in human readable form.
   */
  public void done() {
    out.flush();
    printCsvLog(log);
  }

  public PrintWriter getOut() {
    return out;
  }

  public PrintWriter getLog() {
    return log;
  }

  /**
   * In case I want to print the csv report summary
   * 
   * @param pw
   */
  public void printCsvLog(PrintWriter pw) {
    Preconditions.checkNotNull(pw);
    boolean first = true;
    if (name != null) {
      pw.print(name);
      first = false;
    }

    for (String s : values) {
      if (!first)
        pw.print(",");
      pw.print(s);
      first = false;
    }
    pw.println();
    pw.flush();
  }

  // Javadoc says Runtime.gc is just a hint! Sun's JDK with default GC actually
  // triggers a gc.
  public static void flushMemory() {
    System.gc();
  }

  /**
   * alternate method to force GC. It allocates memory until it gets and
   * OutOfMemoryError, release the data, and then allocates one more time to
   * guarantee a forced GC.
   * 
   * With sun's JDK on default setting System.gc() is apparently sufficient.
   */
  public static void flushMemoryExhaust() {

    // Use a vector to hold the memory.
    Vector<byte[]> v = new Vector<byte[]>();
    int count = 0;
    // increment in megabyte chunks initially
    int size = 1048576;
    // Keep going until we would be requesting
    // chunks of 1 byte
    while (size > 1) {
      try {
        for (; true; count++) {
          // request and hold onto more memory
          v.addElement(new byte[size]);
        }
      }
      // If we encounter an OutOfMemoryError, keep
      // trying to get more memory, but asking for
      // chunks half as big.
      catch (OutOfMemoryError bounded) {
        size = size / 2;
      }
    }
    // Now release everything for GC
    v = null;
    // and ask for a new Vector as a new small object
    // to make sure garbage collection kicks in before
    // we exit the method.
    v = new Vector<byte[]>();
  }

}
