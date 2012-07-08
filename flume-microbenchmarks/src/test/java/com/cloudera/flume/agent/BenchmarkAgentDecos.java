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

import com.cloudera.flume.FlumeBenchmarkHarness;
import com.cloudera.flume.conf.FlumeSpecException;

public class BenchmarkAgentDecos {

  /**
   * Logs output to stderr, this should mute them unless they are serious.
   */
  @Before
  public void muteLogs() {
    Logger.getRootLogger().setLevel(Level.WARN);
  }

  @Test
  public void tiny() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBenchmarkHarness.doDecoBenchmark("nullDeco",
        FlumeBenchmarkHarness.createTinyCases());
    FlumeBenchmarkHarness.doDecoBenchmark("diskFailover",
        FlumeBenchmarkHarness.createTinyCases());
    FlumeBenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        FlumeBenchmarkHarness.createTinyCases());
  }

  @Test
  public void nullDecorator() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBenchmarkHarness.doDecoBenchmark("nullDeco",
        FlumeBenchmarkHarness.createVariedMsgBytesCases());
    FlumeBenchmarkHarness.doDecoBenchmark("nullDeco",
        FlumeBenchmarkHarness.createVariedNumAttrsCases());
    FlumeBenchmarkHarness.doDecoBenchmark("nullDeco",
        FlumeBenchmarkHarness.createVariedValSizeCases());
  }

  @Test
  public void dfoDecorator2() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBenchmarkHarness.doDecoBenchmark("diskFailover",
        FlumeBenchmarkHarness.createVariedMsgBytesCases());
    FlumeBenchmarkHarness.doDecoBenchmark("diskFailover",
        FlumeBenchmarkHarness.createVariedNumAttrsCases());
    FlumeBenchmarkHarness.doDecoBenchmark("diskFailover",
        FlumeBenchmarkHarness.createVariedValSizeCases());
  }

  @Test
  public void e2eDecorator() throws FlumeSpecException, IOException,
      InterruptedException {
    FlumeBenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        FlumeBenchmarkHarness.createVariedMsgBytesCases());
    FlumeBenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        FlumeBenchmarkHarness.createVariedNumAttrsCases());
    FlumeBenchmarkHarness.doDecoBenchmark("ackedWriteAhead",
        FlumeBenchmarkHarness.createVariedValSizeCases());
  }

}
