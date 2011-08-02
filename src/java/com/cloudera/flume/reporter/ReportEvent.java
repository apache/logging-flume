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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.flume.reporter.server.FlumeReport;
import com.google.common.base.Preconditions;

/**
 * Reports are just events that have some helpers for quickly outputting data in
 * human readable or machine parseable formats.
 * 
 * For now, we use a convention where a attribute starting with "rpt." is a a
 * reported field. Later we may include a avro schema or something to include
 * type information as well.
 */
public class ReportEvent extends EventImpl {
  final static Logger LOG = Logger.getLogger(ReportEvent.class);

  final public static String R_SOURCE = "sink";
  final public static String A_COUNT = "count";

  final Map<String, Long> longMetrics = new HashMap<String, Long>();
  final Map<String, Double> doubleMetrics = new HashMap<String, Double>();
  final Map<String, String> stringMetrics = new HashMap<String, String>();

  /**
   * Set a long-valued metric to supplied value. NOT THREAD SAFE.
   */
  public void setLongMetric(String name, long value) {
    longMetrics.put(name, value);
  }

  /**
   * Set a double-valued metric to supplied value. NOT THREAD SAFE.
   */
  public void setDoubleMetric(String name, double value) {
    doubleMetrics.put(name, value);
  }

  /**
   * Set a string-valued metric to supplied value. NOT THREAD SAFE.
   */
  public void setStringMetric(String name, String value) {
    stringMetrics.put(name, value);
  }

  /**
   * Returns the value of a long-valued metric. NOT THREAD SAFE.
   */
  public Long getLongMetric(String name) {
    return longMetrics.get(name);
  }

  /**
   * Returns the value of a double-valued metric. NOT THREAD SAFE.
   */
  public Double getDoubleMetric(String name) {
    return doubleMetrics.get(name);
  }

  /**
   * Returns the value of a string-valued metric. NOT THREAD SAFE.
   */
  public String getStringMetric(String name) {
    return stringMetrics.get(name);
  }

  /**
   * Returns an unmodifiable map of all double metrics
   */
  public Map<String, Double> getAllDoubleMetrics() {
    return Collections.unmodifiableMap(doubleMetrics);
  }

  /**
   * Returns an unmodifiable map of all string metrics
   */
  public Map<String, String> getAllStringMetrics() {
    return Collections.unmodifiableMap(stringMetrics);
  }

  /**
   * Returns an unmodifiable map of all long metrics
   */
  public Map<String, Long> getAllLongMetrics() {
    return Collections.unmodifiableMap(longMetrics);
  }

  /**
   * Returns the total number of metrics. NOT THREAD SAFE.
   */
  public long getNumMetrics() {
    return this.doubleMetrics.size() + this.longMetrics.size()
        + this.stringMetrics.size();
  }

  /**
   * Constructs a copy of event e
   */
  public ReportEvent(ReportEvent e) {
    super(new byte[0]);
    Preconditions.checkNotNull(e);
    this.merge(e);
  }

  public ReportEvent(String src) {
    super(new byte[0]); // empty body
    this.setStringMetric(R_SOURCE, src);
  }

  /**
   * Constructs a new ReportEvent which has the same metrics as the supplied
   * Thrift-style FlumeReport
   */
  public ReportEvent(FlumeReport r) {
    super(new byte[0]);
    Preconditions.checkNotNull(r);
    this.longMetrics.putAll(r.getLongMetrics());
    this.stringMetrics.putAll(r.getStringMetrics());
    this.doubleMetrics.putAll(r.getDoubleMetrics());
  }

  /**
   * Serialises event as JSON string
   */
  public void toJson(Writer o) throws IOException {
    PrintWriter pw = new PrintWriter(o);
    pw.print("{");

    // an inconsistency: Some of the "attributes" are not in the attribute hash
    // table -- time, priority, host, body.
    pw.print("\"host\" : \"" + StringEscapeUtils.escapeJava(getHost()) + "\"");

    // get the attributes and output them
    for (String attr : getAttrs().keySet()) {
      String v = Attributes.toStringStrict(this, attr);
      Type t = Attributes.getType(attr);

      pw.print(", \"" + StringEscapeUtils.escapeJava(attr) + "\" : ");
      if (t != null && t == Type.STRING) {
        pw.print("\"" + StringEscapeUtils.escapeJava(v) + "\"");
      } else {
        pw.print(StringEscapeUtils.escapeJava(v));
      }
    }

    for (Entry<String, String> entry : getAllStringMetrics().entrySet()) {
      pw.print(", \"" + StringEscapeUtils.escapeJava(entry.getKey()) + "\" : ");
      pw.print("\"" + StringEscapeUtils.escapeJava(entry.getValue()) + "\"");
    }

    for (Entry<String, Double> entry : getAllDoubleMetrics().entrySet()) {
      pw.print(", \"" + StringEscapeUtils.escapeJava(entry.getKey()) + "\" : ");
      pw.print(entry.getValue());
    }

    for (Entry<String, Long> entry : getAllLongMetrics().entrySet()) {
      pw.print(", \"" + StringEscapeUtils.escapeJava(entry.getKey()) + "\" : ");
      pw.print(entry.getValue());
    }

    pw.println("}");
  }

  /**
   * Defaults to printing data out as a bunch of html table cells.
   */
  public void toHtml(Writer o) throws IOException {
    PrintWriter pw = new PrintWriter(o);
    pw.print("<table>");

    pw.print("<tr>");
    pw.print("<th>host</th>");
    pw.print("<td>");
    pw.print(getHost());
    pw.println("</td>");
    pw.print("</tr>");

    // get the attributes, filter, sort and output them
    SortedMap<String, String> reportAttrs = new TreeMap<String, String>();

    for (String attr : getAttrs().keySet()) {
      reportAttrs.put(attr, Attributes.toString(this, attr));
    }

    for (Entry<String, Long> e : getAllLongMetrics().entrySet()) {
      reportAttrs.put(e.getKey(), e.getValue().toString());
    }

    for (Entry<String, Double> e : getAllDoubleMetrics().entrySet()) {
      reportAttrs.put(e.getKey(), e.getValue().toString());
    }

    for (Entry<String, String> e : getAllStringMetrics().entrySet()) {
      reportAttrs.put(e.getKey(), e.getValue());
    }

    for (Entry<String, String> a : reportAttrs.entrySet()) {
      pw.println("<tr><th>" + a.getKey() + "</th>");
      pw.print("<td>");
      pw.print("<div class=\"" + a.getKey() + "\">");
      pw.print(a.getValue());
      pw.print("</div>");
      pw.println("</td>");
      pw.println("</tr>");
    }

    pw.print("</table>");
  }

  /**
   * Returns event in string form: host [ priority date ] {attr1:val1}
   * {attr2:val2} {metric1:val1} {metric2:val2} body
   */
  public String toString() {
    StringBuilder metrics = new StringBuilder();

    // Iterate over strings, doubles and longs building a { key : value } string
    SortedMap<String, String> sortedStrings = new TreeMap<String, String>(this
        .getAllStringMetrics());
    for (Entry<String, String> e : sortedStrings.entrySet()) {
      metrics.append("{ " + e.getKey() + " : ");

      String o = e.getValue();
      metrics.append(o + " } ");
    }

    SortedMap<String, Double> sortedDoubles = new TreeMap<String, Double>(this
        .getAllDoubleMetrics());
    for (Entry<String, Double> e : sortedDoubles.entrySet()) {
      metrics.append("{ " + e.getKey() + " : ");

      String o = e.getValue().toString();
      metrics.append(o + " } ");
    }

    SortedMap<String, Long> sortedLongs = new TreeMap<String, Long>(this
        .getAllLongMetrics());
    for (Entry<String, Long> e : sortedLongs.entrySet()) {
      metrics.append("{ " + e.getKey() + " : ");

      String o = e.getValue().toString();
      metrics.append(o + " } ");
    }

    // This implementation taken from EventImpl
    String mbody = StringEscapeUtils.escapeJava(new String(getBody()));
    StringBuilder attrs = new StringBuilder();
    SortedMap<String, byte[]> sorted = new TreeMap<String, byte[]>(this.fields);
    for (Entry<String, byte[]> e : sorted.entrySet()) {
      attrs.append("{ " + e.getKey() + " : ");

      String o = Attributes.toString(this, e.getKey());
      attrs.append(o + " } ");
    }

    return getHost() + " [" + getPriority().toString() + " "
        + new Date(getTimestamp()) + "] " + attrs.toString()
        + metrics.toString() + mbody;

  }

  /**
   * Serialises event as text to supplied writer.
   */
  public void toText(Writer o) throws IOException {
    o.write(StringEscapeUtils.escapeJava(this.toString()));
  }

  /**
   * Legacy report takes a arbitrary string and turns in into a report.
   */
  static class LegacyReport extends ReportEvent {
    static final String ATTR_LEGACY = "report.legacy.html";
    static {
      Attributes.register(ATTR_LEGACY, Type.STRING);
    }

    LegacyReport(String s, byte[] data) {
      super(s);
      set(ATTR_LEGACY, data);
    }

    public void toHtml(Writer w) {
      PrintWriter pw = new PrintWriter(w);
      byte[] data = get(ATTR_LEGACY);
      pw.print(new String(data));
    }
  }

  /**
   * This is a temporary method present while I convert old reports into new
   * ones.
   */
  public static ReportEvent createLegacyHtmlReport(String name, String data) {
    return new LegacyReport(name, data.getBytes());
  }

  /**
   * Does the work for the merge methods by iterating over the set of metrics in
   * e and copying them to this event with a prefix.
   */
  protected void mergeWithPrefix(String prefix, ReportEvent e) {
    for (Entry<String, Double> entry : e.getAllDoubleMetrics().entrySet()) {
      String newKey = prefix + entry.getKey();
      if (getDoubleMetric(newKey) == null) {
        setDoubleMetric(newKey, entry.getValue());
      }
    }

    for (Entry<String, String> entry : e.getAllStringMetrics().entrySet()) {
      String newKey = prefix + entry.getKey();
      if (getStringMetric(newKey) == null) {
        setStringMetric(newKey, entry.getValue());
      }
    }

    for (Entry<String, Long> entry : e.getAllLongMetrics().entrySet()) {
      String newKey = prefix + entry.getKey();
      if (getLongMetric(newKey) == null) {
        setLongMetric(newKey, entry.getValue());
      }
    }
  }

  /**
   * This method merges in values from other events that are not present in the
   * current event.
   */
  public void merge(ReportEvent e) {
    super.merge(e);
    mergeWithPrefix("", e);
  }

  /**
   * This method "hierarchically" merges the attributes of another event
   * prefixing each attribute with the specified prefix.
   * 
   * So if our current event has attributes (a,b,c), and the one being merged in
   * has attributes (d,e,f), and we have prefix be x, the result event will have
   * attributes (a,b,c, x.d, x.e, x.f)
   * 
   * This can be recursive, so if if d was actually an event with (h,i,j) and
   * prefix in that call is y, we could end up with (a,b,c,x.d,x.e, x.f, x.y.h,
   * x.y.i, x.y.j)
   */
  public void hierarchicalMerge(String prefix, ReportEvent e) {
    // Copy the attributes
    super.hierarchicalMerge(prefix, e);
    // Copy the metrics
    mergeWithPrefix(prefix + ".", e);
  }

  /**
   * Return escaped String serialisation of this event
   */
  public String toText() {
    return StringEscapeUtils.escapeJava(this.toString());
  }

  /**
   * Return event as JSON string
   */
  public String toJson() {
    StringWriter sw = new StringWriter();
    try {
      toJson(sw);
    } catch (IOException e) {
      LOG.error("String writer should never throw IOException", e);
      return null;
    }
    return sw.getBuffer().toString();
  }

  /**
   * Returns event as HTML string
   */
  public String toHtml() {
    StringWriter sw = new StringWriter();
    try {
      toHtml(sw);
    } catch (IOException e) {
      LOG.error("String writer should never throw IOException", e);
      return null;
    }
    return sw.getBuffer().toString();
  }
}
