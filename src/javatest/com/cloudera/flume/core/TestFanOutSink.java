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
package com.cloudera.flume.core;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;

public class TestFanOutSink {

  /**
   * Verify that events are independent down each fanout path
   * 
   * @throws InterruptedException
   */
  @Test
  public void testIndependentEvents() throws FlumeSpecException, IOException,
      InterruptedException {

    String spec = "[ { value(\"foo\",\"bar\") => null}, { value(\"foo\",\"bar\" ) => null } ] ";
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    snk.open();
    snk.append(new EventImpl("event body".getBytes()));

  }
}
