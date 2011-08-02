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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

/**
 * This test Attributes, type formatters. Json output is actually read by a json
 * parser to check it.
 */
public class TestReportEvent {

  ReportEvent e;

  @Before
  public void setUp() {
    System.out.println("\n---");
    e = new ReportEvent("test");
    e.setLongMetric("rpt.long.value", 123456);
    e.setStringMetric("rpt.string.value", "this is a test string");
    e.setDoubleMetric("rpt.double.value", -1234.32e2);
    e.setLongMetric("event1.duplicateLong", 54321L);
  }

  @Test
  public void testReportHtml() throws IOException {
    Writer w = new OutputStreamWriter(System.out);
    e.toHtml(w);
    w.flush();
  }

  @Test
  public void testReportJson() throws IOException {
    Writer w = new OutputStreamWriter(System.out);
    e.toJson(w);
    w.flush();
  }

  /**
   * This just checks to make sure the value was json parsable. Throws exception
   * on parse failure.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testReportJsonParse() throws IOException {
    StringWriter w = new StringWriter();
    e.toJson(w);
    w.flush();
    System.out.println(w.getBuffer());
    StringReader r = new StringReader(w.getBuffer().toString());

    ObjectMapper m = new ObjectMapper();
    HashMap<String, Object> node = m.readValue(r, HashMap.class);
    System.out.println(node);
  }

  @Test
  public void testReportText() throws IOException {
    Writer w = new OutputStreamWriter(System.out);
    e.toText(w);
    w.flush();
  }

  @Test
  public void testLegacyReport() throws IOException {
    ReportEvent e2 = ReportEvent.createLegacyHtmlReport("test",
        "this is an old single string report");
    Writer out = new OutputStreamWriter(System.out);
    System.out.println(e2.toText());
    System.out.println();

    e2.toText(out);
    out.flush();
    System.out.println();

    e2.toHtml(out);
    out.flush();
    System.out.println();

    e2.toJson(out);
    out.flush();
    System.out.println();
  }

  /**
   * Only the new "another" attribute should be added to the original report
   */
  @Test
  public void testMerge() throws IOException {
    ReportEvent e1 = new ReportEvent("test");
    e1.setLongMetric("another", 12345);

    long attrs = e.getNumMetrics();
    System.out.println("- before merge, " + attrs + " attributes");
    Writer out = new OutputStreamWriter(System.out);
    e.toJson(out);
    out.flush();

    System.out.println("- after merge, " + attrs + " + 1  attributes");
    e.merge(e1);
    e.toJson(out);
    out.flush();
    assertEquals(attrs + 1, e.getNumMetrics());
  }

  /**
   * Test that hierarchical merge works correctly - a) that all metrics are
   * merged in correctly, b) that metrics are renamed correctly and c) that
   * merged-in metrics with an existing name are suppressed.
   */
  @Test
  public void testHierarchicalMerge() throws IOException {
    ReportEvent e1 = new ReportEvent("test");
    e1.setLongMetric("another", 12345);
    e1.setLongMetric("duplicateLong", 23456);

    long attrs = e.getNumMetrics();
    System.out.println("- before merge, " + attrs + " attributes");
    Writer out = new OutputStreamWriter(System.out);
    e.toJson(out);
    out.flush();

    System.out.println("- after merge, " + attrs + " + 2  metrics");
    e.hierarchicalMerge("event1", e1);
    e.toJson(out);
    out.flush();
    assertEquals(attrs + 2, e.getNumMetrics());
    assertEquals(e.getLongMetric("event1.another"), Long.valueOf(12345L));
    assertEquals(e.getLongMetric("event1.duplicateLong"), Long.valueOf(54321L));
  }
}
