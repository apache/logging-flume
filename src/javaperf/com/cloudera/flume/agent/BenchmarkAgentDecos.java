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
package com.cloudera.flume.agent;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.util.BenchmarkHarness;

public class BenchmarkAgentDecos {

  /**
   * Logs output to stderr, this should mute them unless they are serious.
   */
  @Before
  public void muteLogs() {
    Logger.getRootLogger().setLevel(Level.WARN);
  }

  @Test
  public void tiny() throws FlumeSpecException, IOException {
    BenchmarkHarness.doDecoBenchmark("nullDeco", BenchmarkHarness.tiny);
    BenchmarkHarness.doDecoBenchmark("diskFailover", BenchmarkHarness.tiny);
    BenchmarkHarness.doDecoBenchmark("ackedWriteAhead", BenchmarkHarness.tiny);
  }

  @Test
  public void nullDecorator() throws FlumeSpecException, IOException {
    BenchmarkHarness.doDecoBenchmark("nullDeco", BenchmarkHarness.varyMsgBytes);
    BenchmarkHarness.doDecoBenchmark("nullDeco", BenchmarkHarness.varyNumAttrs);
    BenchmarkHarness.doDecoBenchmark("nullDeco", BenchmarkHarness.varyValSize);
  }

  @Test
  public void dfoDecorator2() throws FlumeSpecException, IOException {
    BenchmarkHarness.doDecoBenchmark("diskFailover",
        BenchmarkHarness.varyMsgBytes);
    BenchmarkHarness.doDecoBenchmark("diskFailover",
        BenchmarkHarness.varyNumAttrs);
    BenchmarkHarness.doDecoBenchmark("diskFailover",
        BenchmarkHarness.varyValSize);
  }

  @Test
  public void e2eDecorator() throws FlumeSpecException, IOException {
    BenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        BenchmarkHarness.varyMsgBytes);
    BenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        BenchmarkHarness.varyNumAttrs);
    BenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        BenchmarkHarness.varyValSize);
  }

}
