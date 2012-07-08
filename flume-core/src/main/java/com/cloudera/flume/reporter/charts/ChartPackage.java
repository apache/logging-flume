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
package com.cloudera.flume.reporter.charts;

import com.cloudera.flume.reporter.charts.google.GoogleChartPackage;
import com.cloudera.flume.reporter.histogram.HistogramChartGen;
import com.cloudera.flume.reporter.history.TimelineChartGen;

/**
 * This is singleton object used to construct chart generators. It defaults to
 * GoogleCharts based charts, but can be overridden with other chart generation
 * packages.
 */
abstract public class ChartPackage {
  // Default to google charts for now.
  static ChartPackage pkg = new GoogleChartPackage();

  public static void setChartGenerators(ChartPackage c) {
    pkg = c;
  }

  public static HistogramChartGen<String> createHistogramGen() {
    return pkg.histogramGen();
  }

  public static TimelineChartGen<Long> createTimelineGen() {
    return pkg.timelineGen();
  }

  public abstract HistogramChartGen<String> histogramGen();

  public abstract TimelineChartGen<Long> timelineGen();
}
