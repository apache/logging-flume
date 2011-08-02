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
package com.cloudera.testio;

import org.junit.Test;

import com.cloudera.util.Benchmark;

/**
 * There are three formats for handling blobs of data. Which is the most
 * efficient, which are the most convenient and which are the cheapest to
 * translate to and from?
 * 
 */

// String --------> 1.3s 1.4 1.3 --------> 7.7Mops/s
// StringBuffer --> 1.2s 1.2 1.2 --------> 8.3Mops/s
// StringBuilder -> .75s 0.77 .78 ------> 13.0Mops/s
// StringFormat --> 13.9s 14.3 15.7 -----> 0.7Mops/s

public class PerfStringAppends {

  int times = 10000000;

  String base = "this is a test";

  @Test
  public void testStringAppend() {
    Benchmark b = new Benchmark("String Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      @SuppressWarnings("unused")
      String temp = base + i;
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringBufferAppend() {
    StringBuffer buf = new StringBuffer(base);
    Benchmark b = new Benchmark("StringBuffer Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      buf.append(i);
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringBuilderAppend() {
    StringBuilder buf = new StringBuilder(base);
    Benchmark b = new Benchmark("StringBuffer Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      buf.append(i);
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringFormat() {
    String format = "%s%d";
    Benchmark b = new Benchmark("StringFormatAppend");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      String.format(format, base, i);
    }
    b.mark("after " + times);
    b.done();

  }

  @Test
  public void testStringAppend2() {
    Benchmark b = new Benchmark("String Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      @SuppressWarnings("unused")
      String temp = base + i;
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringBufferAppend2() {
    StringBuffer buf = new StringBuffer(base);
    Benchmark b = new Benchmark("StringBuffer Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      buf.append(i);
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringBuilderAppend2() {
    StringBuilder buf = new StringBuilder(base);
    Benchmark b = new Benchmark("StringBuffer Append");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      buf.append(i);
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringFormat2() {
    String format = "%s%d";
    Benchmark b = new Benchmark("StringFormatAppend");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      String.format(format, base, i);
    }
    b.mark("after " + times);
    b.done();

  }

}
