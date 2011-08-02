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
package com.cloudera.flume.reporter.histogram;

import java.io.IOException;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.charts.ChartPackage;
import com.cloudera.util.Histogram;

/**
 * This sink histograms values. The extract abstract method is used to pull
 * values out of an event and is used to bin events.
 */
abstract public class HistogramSink implements EventSink {
  Histogram<String> h;
  final String name;

  public HistogramSink(String name) {
    this.name = name;
    this.h = new Histogram<String>();
  }

  /**
   * This assumes that we will extract one value from an event. We could
   * actually extract multiple
   */
  abstract public String extract(Event e);

  @Override
  public void append(Event e) throws IOException {
    String t = extract(e);
    // if failed to extract, skip
    if (t != null) {
      h.increment(t);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void open() throws IOException {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ReportEvent getReport() {
    String report = (ChartPackage.createHistogramGen().generate(h) + "<pre>"
        + name + "\n" + h + "</pre>");
    return ReportEvent.createLegacyHtmlReport(name, report);
  }

  public Histogram<String> getHistogram() {
    return h;
  }

}
