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
package com.cloudera.flume.reporter;

import junit.framework.TestCase;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.EventSink;

/**
 * This outputs data for a series of sinks but doens't verify values. This is
 * done by visual inspection of output for now.
 */
public class TestHierarchicalReports extends TestCase {

  public void testSimple() throws FlumeSpecException {
    String s = "console";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);

    assertTrue(r.contains(sink.getName()));
  }

  public void testOneDeco() throws FlumeSpecException {
    String s = "{ stubbornAppend => console}";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
  }

  public void testHierarchy() throws FlumeSpecException {
    String s =
        "{ intervalSampler(5) => { stubbornAppend => { insistentOpen => console } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
    assertTrue(r.contains("IntervalSampler.StubbornAppend.InsistentOpen.sink"));
  }

  public void testWalDeco() throws FlumeSpecException {
    String s =
        "{ ackedWriteAhead => { stubbornAppend => { insistentOpen => console } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);
    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
    assertTrue(r.contains("NaiveFileWAL.StubbornAppend.InsistentOpen.sink"));
  }

  public void testWrappedWal() throws FlumeSpecException {
    String s =
        "{ insistentOpen => { ackedWriteAhead => { stubbornAppend => { insistentOpen => console } } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);
    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
    assertTrue(r
        .contains("InsistentOpen.NaiveFileWAL.StubbornAppend.InsistentOpen.sink"));
  }

  public void testFailover() throws FlumeSpecException {
    String s = " { ackedWriteAhead => < thrift ? console > } ";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
    assertTrue(r
        .contains("NaiveFileWAL.BackoffFailover[primary].sink : ThriftEventSink"));

    assertTrue(r
        .contains("NaiveFileWAL.BackoffFailover[backup].sink : ConsoleEventSink"));
  }

  public void testMultiple() throws FlumeSpecException {
    String s = " { ackedWriteAhead => [ thrift , console ] } ";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    ReportEvent rpt = sink.getReport();
    String r = rpt.toText();
    System.out.println(r);
    assertTrue(r.contains(sink.getName() + ".sink"));
    assertTrue(r.contains("NaiveFileWAL.Fanout[0].ThriftEventSink.sink"));
    assertTrue(r.contains("NaiveFileWAL.Fanout[1].ConsoleEventSink.sink"));
  }
}
