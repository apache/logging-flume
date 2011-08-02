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
package com.cloudera.flume.reporter.builder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.reporter.histogram.RegexGroupHistogramSink;

/**
 * We have a simple text file that contains many regex expressions and group
 * indexes. This generates a list of regexgroup histogrammers.
 * 
 * The format:
 * 
 * "<name>:<type>:<idx>:<regex>\n"
 * 
 * where <name> is the name of the chart/reporter, <type> will allow users to
 * specify chart configuration options (currently ignored), <idx> is some
 * integer, and <regex> is a regular expression (no newlines allows currently).
 * 
 */
public class SimpleRegexReporterBuilder extends
    ReporterBuilder<RegexGroupHistogramSink> {

  String fname;

  public SimpleRegexReporterBuilder(String f) {
    this.fname = f;
  }

  // (\w+):(\w+):(\d+):(.*)
  Pattern p = Pattern.compile("(\\w+):(\\w+):(\\d+):(.*)");

  @Override
  public Collection<RegexGroupHistogramSink> load() throws IOException {
    List<RegexGroupHistogramSink> l = new ArrayList<RegexGroupHistogramSink>();
    RandomAccessFile raf = new RandomAccessFile(fname, "r");

    String s = raf.readLine();
    while (s != null) {
      Matcher m = p.matcher(s);
      // skip failures.
      if (!m.matches()) {
        System.err.printf("Invalid regex group specification: %s\n", s);
        continue;
      }

      String name = m.group(1);
      // String type = m.group(2); // ignore for now.
      int grp = Integer.parseInt(m.group(3));
      Pattern regex = Pattern.compile(m.group(4));
      RegexGroupHistogramSink rghs = new RegexGroupHistogramSink(name, regex,
          grp);
      l.add(rghs);

      s = raf.readLine();
    }
    return l;
  }
}
