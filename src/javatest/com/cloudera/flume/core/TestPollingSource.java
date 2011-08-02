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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This tests a poller source.
 */
public class TestPollingSource {

  /**
   * Output Debugging messages
   */
  @Before
  public void setup() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  /**
   * This setups and executes a poller on the FlumeNode's reporter.
   */
  @Test
  public void testReportPoller() throws FlumeSpecException, IOException {
    SourceBuilder bld = PollingSource.reporterPollBuilder();
    EventSource src = bld.build("50");
    EventSink snk =
        new CompositeSink(new ReportTestingContext(), "[ console , counter(\"count\") ]");
    src.open();
    snk.open();
    for (int i = 0; i < 10; i++) {
      Event e = src.next();
      snk.append(e);
    }
    snk.close();
    src.close();
    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    assertEquals(10, cnt.getCount());
  }
}
