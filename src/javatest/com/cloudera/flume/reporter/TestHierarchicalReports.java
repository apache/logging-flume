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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.EventSink;

/**
 * This outputs data for a series of sinks but doens't verify values. This is
 * done by visual inspection of output for now.
 */
public class TestHierarchicalReports {

  @Test
  public void testSimple() throws FlumeSpecException {
    String s = "console";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);

    Assert.assertTrue(r.contains("X." + sink.getName()));
  }

  @Test
  public void testOneDeco() throws FlumeSpecException {
    String s = "{ stubbornAppend => console}";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
  }

  @Test
  public void testHierarchy() throws FlumeSpecException {
    String s =
        "{ intervalSampler(5) => { stubbornAppend => { insistentOpen => console } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
    Assert.assertTrue(r.contains("X.IntervalSampler.StubbornAppend.InsistentOpen"));
  }

  @Test
  public void testWalDeco() throws FlumeSpecException {
    String s =
        "{ ackedWriteAhead => { stubbornAppend => { insistentOpen => console } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
    Assert.assertTrue(r.contains("X.NaiveFileWAL.StubbornAppend.InsistentOpen"));
  }

  @Test
  public void testWrappedWal() throws FlumeSpecException {
    String s =
        "{ insistentOpen => { ackedWriteAhead => { stubbornAppend => { insistentOpen => console } } } }";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
    Assert.assertTrue(r
        .contains("X.InsistentOpen.NaiveFileWAL.StubbornAppend.InsistentOpen"));
  }

  @Test
  public void testFailover() throws FlumeSpecException {
    String s = " { ackedWriteAhead => < thriftSink ? console > } ";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
    Assert.assertTrue(r
        .contains("X.NaiveFileWAL.BackoffFailover.primary.ThriftEventSink"));

    Assert.assertTrue(r
        .contains("X.NaiveFileWAL.BackoffFailover.backup.ConsoleEventSink"));
  }

  @Test
  public void testMultiple() throws FlumeSpecException {
    String s = " { ackedWriteAhead => [ thriftSink , console ] } ";
    EventSink sink = FlumeBuilder.buildSink(new Context(), s);

    Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();
    sink.getReports("X.", reports);
    String r = "";
    for (Entry<String, ReportEvent> e : reports.entrySet()) {
      r += e.getKey() + " = " + e.getValue().toText();
    }
    System.out.println(r);
    Assert.assertTrue(r.contains("X." + sink.getName()));
    Assert.assertTrue(r.contains("X.NaiveFileWAL.Fanout.0.ThriftEventSink"));
    Assert.assertTrue(r.contains("X.NaiveFileWAL.Fanout.1.ConsoleEventSink"));
  }
}
