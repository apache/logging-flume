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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Driver.DriverState;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This just uses the benchmark decorators and requires users to manualy inspect
 * them to see if values are reasonable.
 */
public class TestBenchmarkDeco implements ExampleData {

  @Test
  public void testSimpleMem() throws IOException, InterruptedException {

    EventSource src = new NoNlASCIISynthSource(25, 100, 1);
    EventSink snk = new ConsoleEventSink();
    EventSink snk2 = new BenchmarkReportDecorator<EventSink>("report", snk);
    EventSink snk3 = new BenchmarkInjectDecorator<EventSink>(snk2);
    EventSink snk4 = new InMemoryDecorator<EventSink>(snk3);

    DirectDriver connect = new DirectDriver(src, snk4);
    connect.start();
    assertTrue(connect.join(5000));
    snk2.getMetrics().toText(new OutputStreamWriter(System.err));
  }

  @Test
  public void testSimple() throws IOException, InterruptedException {
    EventSource src = new NoNlASCIISynthSource(25, 100, 1);
    EventSink snk = new ConsoleEventSink();
    EventSink snk2 = new BenchmarkReportDecorator<EventSink>("report", snk);
    EventSink snk3 = new BenchmarkInjectDecorator<EventSink>(snk2);

    DirectDriver connect = new DirectDriver(src, snk3);
    connect.start();
    assertTrue(connect.join(5000));
    snk2.getMetrics().toText(new OutputStreamWriter(System.err));
  }

  @Test
  public void testSimpleBuilder() throws FlumeSpecException {
    String spec = "{benchinject => null}";
    FlumeBuilder.buildSink(new Context(), spec);
  }

  @Test
  public void testSimpleBuilder2() throws FlumeSpecException {
    String spec = "{benchreport(\"report\") => null}";
    FlumeBuilder.buildSink(new Context(), spec);
  }

  @Test
  public void testBuildReportSink2() throws FlumeSpecException {
    String spec = "{benchreport(\"report\", \"text(\\\"test\\\")\") => null } ";
    FlumeBuilder.buildSink(new Context(), spec);
  }

  @Test
  public void testBuildInjectDeco() throws FlumeSpecException {
    String spec = "{benchinject(\"report\") => null}";
    FlumeBuilder.buildSink(new Context(), spec);
  }

  /**
   * Tests to make sure the report sink receives data.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testReportSink() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "{benchinject(\"foo\") => {benchreport(\"report\", \"[ console , counter(\\\"test\\\") ]\")  => null } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);
    snk.open();
    snk.append(new EventImpl(new byte[0]));
    snk.append(new EventImpl(new byte[0]));
    snk.close();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("test");
    Assert.assertEquals(1, ctr.getCount());

  }

}
