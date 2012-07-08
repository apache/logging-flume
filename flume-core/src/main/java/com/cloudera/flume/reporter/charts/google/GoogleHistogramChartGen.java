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

import java.util.SortedSet;

import com.cloudera.flume.reporter.histogram.HistogramChartGen;
import com.cloudera.util.Histogram;
import com.cloudera.util.Pair;

/**
 * Google charts has quite a few string encoding things to compact the size of
 * the url used to generate the chart.
 * 
 * Type T is the class used to bin values into histograms.
 */
class GoogleHistogramChartGen<T> extends HistogramChartGen<T> {
  final public static String BASE_URL = "http://chart.apis.google.com/chart?";

  int height, width;
  String title;
  boolean scaled = true;

  public GoogleHistogramChartGen(String title, int height, int width,
      boolean scaled) {
    this.title = title;
    this.height = height;
    this.width = width;
    this.scaled = scaled;
  }

  public GoogleHistogramChartGen(String title, int height, int width) {
    this(title, height, width, true);
  }

  public GoogleHistogramChartGen(String title) {
    this(title, 300, 300);
  }

  public GoogleHistogramChartGen() {
    this(null, 300, 300);
  }

  private String size() {
    return "chs=" + width + "x" + height;
  }

  // Text encoding
  // This is limited to values 0-100 inclusive. If a value is > 100, it is
  // marked as 100.
  // API says good for about 500 pixel graphs.
  private String histoSortFreqs(Histogram<T> h) {
    if (h.size() == 0)
      return ""; // No data

    SortedSet<Pair<T, Integer>> s = h.sorted();
    StringBuilder data = null;
    for (Pair<T, Integer> p : s) {
      if (data == null) {
        data = new StringBuilder();
        data.append(p.getRight());
      } else {
        data.append("," + p.getRight());
      }
    }
    return data.toString();
  }

  // Text encoding with data scaling
  private String histoSortFreqsS(Histogram<T> h) {
    if (h.size() == 0)
      return ""; // No data

    SortedSet<Pair<T, Integer>> s = h.sorted();
    String data = null;
    int max = 0;
    int sum = 0;
    int cnt = 0;
    for (Pair<T, Integer> p : s) {
      max = p.getRight() > max ? p.getRight() : max;
      cnt++;
      sum += p.getRight();
      if (data == null) {
        data = "" + p.getRight();
      } else {
        data += "," + p.getRight();
      }
    }

    // truncate y axis at avg
    int avg = sum / cnt;
    String limits = "&chds=0," + avg;
    // add axis
    String axis = "&chxt=y,x&chxl=0:|0|" + avg + "|1:|0|" + cnt; // last doesn't
    // have a '|'

    String charttype = "&cht=bvg";
    if (cnt > 10) {
      charttype = "&cht=lc";
    }

    return charttype + "&chd=t:" + data + limits + axis;
  }

  @Override
  public String generate(Histogram<T> h) {
    String title = "";
    if (this.title != null) {
      // TODO (check string, need to escape spaces with '+').
      title = "&chtt=" + this.title;
    }

    String data = scaled ? histoSortFreqsS(h) : histoSortFreqs(h);

    return "<img src=\"" + BASE_URL + size() + title + data + "\" />";
  }

}
