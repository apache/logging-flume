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

import java.nio.ByteBuffer;

import org.junit.Test;

import com.cloudera.util.Benchmark;

/**
 * This is a performance test to see how expensive different string/byte
 * translations cost
 */
public class PerfStringTrans  {
  int times = 10000000;

  String base = "this is a test";

  @Test
  public void testStringToByteArray() {
    Benchmark b = new Benchmark("String to ByteArray");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      base.getBytes();
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringToWrapByteBuffer() {
    Benchmark b = new Benchmark("String wrap ByteBuf");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      ByteBuffer.wrap(base.getBytes());
    }
    b.mark("after " + times);
    b.done();

  }

  @Test
  public void testStringtoByteBufferCopy() {
    Benchmark b = new Benchmark("String alloc ByteBuf");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      byte[] buf = base.getBytes();
      ByteBuffer bb = ByteBuffer.allocate(buf.length);
      bb.put(buf);
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringToByteArray2() {
    Benchmark b = new Benchmark("String to ByteArray");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      base.getBytes();
    }
    b.mark("after " + times);
    b.done();
  }

  @Test
  public void testStringToWrapByteBuffer2() {
    Benchmark b = new Benchmark("String wrap ByteBuf");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      ByteBuffer.wrap(base.getBytes());
    }
    b.mark("after " + times);
    b.done();

  }

  @Test
  public void testStringtoByteBufferCopy2() {
    Benchmark b = new Benchmark("String alloc ByteBuf");

    b.mark("begin");
    for (int i = 0; i < times; i++) {
      byte[] buf = base.getBytes();
      ByteBuffer bb = ByteBuffer.allocate(buf.length);
      bb.put(buf);
    }
    b.mark("after " + times);
    b.done();
  }

}
