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
package com.cloudera.flume.reporter.charts.google;

import java.util.List;

import com.cloudera.flume.reporter.history.TimelineChartGen;
import com.cloudera.util.Pair;

/**
 * This generates a google charts html tag that generates a line graph based on
 * a list of <timestamp,count>.
 * 
 * The data set's max y value is set to hover around 2/3 of the height of the
 * graph.
 * 
 * Go here for the details behind the magic incantations.
 * http://code.google.com/apis/chart/
 */
class GoogleTimelineChartGen extends TimelineChartGen<Long> {

  final String title;

  final int height, width;

  public GoogleTimelineChartGen(String title, int height, int width) {
    this.title = title;
    this.height = height;
    this.width = width;
  }

  public GoogleTimelineChartGen(String title) {
    this(title, 300, 300);
  }

  public GoogleTimelineChartGen() {
    this(null, 300, 300);
  }

  private String size() {
    // chs == chart size
    return "chs=" + width + "x" + height;
  }

  @Override
  public String generate(List<Pair<Long, Long>> history) {

    String title = "";
    if (this.title != null) {
      // chtt is chart title
      // TODO (jon) check title string; need to escape spaces with '+').
      title = "&chtt=" + this.title;
    }
    String data = timeline(history);

    return "<img src=\"" + GoogleHistogramChartGen.BASE_URL + size() + title + data
        + "\" />";
  }

  // Text encoding with data scaling
  // Left element is the timestamp, and the right element is a count value.
  // If there are >10 entries, we use a line chart, otherwises a bar graph
  // 
  // TODO (jon) Right now we ignore the timestamp (assuming each time step is
  // equal)
  private String timeline(List<Pair<Long, Long>> h) {
    StringBuilder data = new StringBuilder();
    long max = 0;
    long sum = 0;
    long cnt = h.size();

    if (cnt == 0) {
      return ""; // No Data!
    }

    for (Pair<Long, Long> p : h) {
      max = p.getRight() > max ? p.getRight() : max;
      cnt++;
      sum += p.getRight();
      if (data.length() == 0) {
        data.append(p.getRight());
      } else {
        data.append("," + p.getRight());
      }
    } 

    // truncate y axis at avg
    // make max hover around 2/3s of the way up the chart.
    // chds == chart data set limits
    String limits = "&chds=0," + (max * 3 / 2); // chds = chart dimensions

    // add axis, last entry doesn't have a '|'
    // chxt == chart axes == y is left, x is bottom.
    // chxl == chart axis labels
    String axis = "&chxt=y,x&chxl=0:|0|" + (max * 3 / 2) + "|1:|0|" + cnt;

    // cht=bvg == chart type is vertical bar graph
    String charttype = "&cht=bvg";

    if (cnt > 10) {
      // cht=lc == chart type is linechart
      charttype = "&cht=lc";
    }

    // chd = chart data string
    return charttype + "&chd=t:" + data.toString() + limits + axis;
  }

}
