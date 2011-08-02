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
package com.cloudera.flume.master;

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Benchmark;

public class EvilExhaustConnections extends TestCase {

  /**
   * This is just to see how many open connections a process can have, and to
   * force and out of file handles exception.
   * 
   * Seems to seems to fail around 1000 connections, due to
   * UnkonwnHostExceptions and ConnectionRefused, SocketException due to too
   * many open files
   * 
   * @throws FlumeSpecException
   */
  public void testTooManyOpens() throws IOException, FlumeSpecException {
    Benchmark b = new Benchmark("connection exhaust");

    EventSource src = FlumeBuilder.buildSource("thrift(31337)");

    EventSink snk = FlumeBuilder.buildSink("thrift(\"0.0.0.0\",31337)");

    src.open();

    // iterate until an exception is thrown
    int i = 0;
    try {
      for (i = 0; true; i++) {

        snk.open();
        System.out.println(i + " connections...");
      }
    } catch (IOException io) {
      System.out.println(io);
      b.mark("conns", i);
    }
    src.close();
    b.done();
  }

  public void testSurviveManyOpens() throws IOException, FlumeSpecException {
    Benchmark b = new Benchmark("connection exhaust");

    EventSource src = FlumeBuilder.buildSource("thrift(31337)");
    src.open();

    // iterate until an exception is thrown
    for (int i = 0; i < 10000; i++) { // previous fails at 1000, make sure ok at
      // an order of magnitude bigger.

      EventSink snk = FlumeBuilder.buildSink("thrift(\"0.0.0.0\",31337)");
      snk.open();
      System.out.println(i + " connections...");
      snk.close();
    }

    src.close();
    b.done();
  }
}
