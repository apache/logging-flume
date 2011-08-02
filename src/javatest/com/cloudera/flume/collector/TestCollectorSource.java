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
package com.cloudera.flume.collector;

import junit.framework.TestCase;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;

/**
 * So far this tests the builder and repeated opens and closes.
 */
public class TestCollectorSource extends TestCase {

  public void testBuilder() throws FlumeSpecException {
    String src = "collectorSource";
    FlumeBuilder.buildSource(src);

    String src2 = "collectorSource(5150)";
    FlumeBuilder.buildSource(src2);

    try {
      FlumeBuilder.buildSource("collectorSource(99, \"red luftballoons\")");
    } catch (Exception e) {
      return;
    }
    fail("should have caught aproblem");
  }

  /**
   * TODO (jon) Fix the thrift server?
   * 
   * This test fails occasionally. There is an issue in the thrift server being
   * used under the covers. It serve() method does not provide any hooks to gain
   * notification between listen() and accept() (e.g. when I consider it open.)
   * Also the start and close method of the thrift server is asynchronous -
   * there is no hook for me to determine when the server is actually closed.
   * 
   * These omissions mean that this test is not reliable due to port contention
   * (the next open start before the previous close finishes), and due to races
   * (close starts before open has finished)
   */
  // public void testOpenClose() throws RecognitionException,
  // FlumeSpecException,
  // IOException {
  // String src = "collectorSource";
  //
  // for (int i = 0; i < 100; i++) {
  // EventSource esrc = FlumeBuilder.buildSource(src);
  // esrc.open();
  // esrc.close();
  // }
  //
  // }
}
