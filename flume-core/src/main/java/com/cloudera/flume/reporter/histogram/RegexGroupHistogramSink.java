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
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.MultiReporter;
import com.cloudera.flume.reporter.builder.SimpleRegexReporterBuilder;
import com.google.common.base.Preconditions;

/**
 * This takes a regex and a group index and generates a histogram based on the
 * value extracted. Values that do not match are not counted.
 * 
 * For example: the group index and regex combo of: 3, (\d+):(\d+):(\d+)
 * 
 * for the following values: 123:456:789, abc:def:xyz, 11:22:33, 55:66:33
 * 
 * would result in a histogram with (value, count) : (789, 1), (33,2).
 * 
 * NOTE: the NFA-based regex algorithm used by java.util.regex.* (and in this
 * class) is slow and does not scale. It is fully featured but has an
 * exponential worst case running time. This will be replaced with a faster but
 * more memory hungry and less featured DFA-based regex algorithm. (We will lose
 * capture groups).
 */
public class RegexGroupHistogramSink extends HistogramSink {
  Pattern pat;
  int grp;

  public RegexGroupHistogramSink(String name, Pattern pat, int grp) {
    super(name);
    this.pat = pat;
    this.grp = grp;
  }

  @Override
  public String extract(Event e) {
    String s = new String(e.getBody());
    Matcher m = pat.matcher(s);
    if (m.find()) {
      return m.group(grp);
    }
    return null;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "usage: regexhistospec(regexspecfile)");

        String fname = argv[0];
        SimpleRegexReporterBuilder srrb = new SimpleRegexReporterBuilder(fname);
        Collection<RegexGroupHistogramSink> sinks;
        try {
          sinks = srrb.load();
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to create regex report from spec file " + fname + ": "
                  + e);
        }
        if (sinks.size() == 1)
          return sinks.iterator().next();

        EventSink snk = new MultiReporter(fname, sinks);
        return snk;
      }
    };
  }

  public static SinkBuilder builderSimple() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 3,
            "usage: regexhisto(name, regex, idx)");

        String name = argv[0];
        String regex = argv[1];
        Integer idx = Integer.parseInt(argv[2]);
        Pattern pat = Pattern.compile(regex);

        EventSink snk = new RegexGroupHistogramSink(name, pat, idx);
        return snk;

      }
    };
  }

}
