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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * Simply tests the multiplier. (send a single message n times)
 */
public class TestMultiplierDeco {

  @Test
  public void testMultiplier() throws IOException, InterruptedException {
    final int repeat = 9;
    final int msgs = 10;
    CounterSink cnt = new CounterSink("count");
    // send to counter 10x.
    EventSinkDecorator<CounterSink> s = new MultiplierDecorator<CounterSink>(
        cnt, repeat);
    s.open();
    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("" + i).getBytes());
      s.append(e);
    }

    Assert.assertEquals(msgs * repeat, cnt.getCount());
  }

  /**
   * Test the builder interface.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testMultiplierBuilder() throws IOException, FlumeSpecException,
      InterruptedException {
    final int repeat = 7;
    final int msgs = 10;

    String cfg = "{ mult(" + repeat + ") => counter(\"count\") }";
    EventSink s = FlumeBuilder.buildSink(new ReportTestingContext(), cfg);
    s.open();

    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("" + i).getBytes());
      s.append(e);
    }

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    Assert.assertEquals(msgs * repeat, cnt.getCount());
  }

  /**
   * Test the builder interface. Makes sure the multiplier doesn't multiply the
   * benchmark messages.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testBenchmarkMultiplierBuilder() throws IOException,
      FlumeSpecException, InterruptedException {
    final int repeat = 3;
    final int msgs = 4;

    String cfg = "{ benchinject => { mult(" + repeat
        + ") => [console, counter(\"count\")] }}";
    EventSink s = FlumeBuilder.buildSink(new ReportTestingContext(), cfg);
    s.open();

    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("" + i).getBytes());
      s.append(e);
    }
    s.close();

    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");
    // +3 -> start, first, stop
    Assert.assertEquals(msgs * repeat + 3, cnt.getCount());

  }
}
