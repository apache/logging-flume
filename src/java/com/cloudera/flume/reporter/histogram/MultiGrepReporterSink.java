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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.arabidopsis.ahocorasick.AhoCorasick;
import org.arabidopsis.ahocorasick.SearchResult;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.MultiReporter;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.builder.MultiGrepReporterBuilder;
import com.cloudera.flume.reporter.charts.ChartPackage;
import com.cloudera.util.Histogram;
import com.google.common.base.Preconditions;

/**
 * This uses an AhoCorasick multi string search state machine to search the body
 * of events. It essentially takes a set of strings and makes them into a large
 * trie that allows for a one pass traversal that can result in many substring
 * matches. Since this structure can return multiple matches, we increment hit
 * counts by 1 per event.
 * 
 * Example:
 * 
 * searching for : foo, fool, foolish, bar, barish
 * 
 * in the event: "barfing foo at the bar was foolish."
 * 
 * Would increment by 1: bar, foo, fool, foolish. 'bar' and 'foo' are
 * incremented once although there were two hits. 'fool', and 'foolish' are each
 * incremented once.
 * 
 * We can't use the other HistogramSink because this extractor pulls out
 * multiple values instead of single values (tags vs strict categorization).
 */
public class MultiGrepReporterSink<T> extends EventSink.Base {

  final String name;
  final AhoCorasick<T> aho;
  final Histogram<String> histo;
  final HistogramChartGen<String> chartgen;

  /**
   * This will default to returning the string on a match.
   */
  public MultiGrepReporterSink(String name, AhoCorasick<T> aho) {
    this.name = name;
    this.aho = aho;
    this.histo = new Histogram<String>();
    this.chartgen = ChartPackage.createHistogramGen(); // new
    // GoogleHistogramChartGen<T>();
  }

  /**
   * We use this instead of a constructor to build these.
   */
  public static MultiGrepReporterSink<String> build(String name, String[] strs) {
    // build the Aho multistring search Trie structure
    AhoCorasick<String> aho = new AhoCorasick<String>();
    for (String s : strs) {
      // will return Strings to identify matches.
      aho.add(s.getBytes(), s);
    }
    return new MultiGrepReporterSink<String>(name, aho);
  }

  // return a set of matches (do not return duplicates)
  public Collection<T> extract(Event e) {
    Iterator<SearchResult<T>> iter = aho.search(e.getBody());
    Set<T> results = new HashSet<T>();

    while (iter.hasNext()) {
      SearchResult<T> res = iter.next();
      for (T o : res.getOutputs()) {
        results.add(o);
      }
    }
    return results;
  }

  @Override
  public void append(Event e) throws IOException {
    Collection<T> ts = extract(e);
    // if failed to extract, skip
    for (T t : ts) {
      histo.increment(t.toString());
    }
    super.append(e);
  }

  @Override
  public void open() throws IOException {
    aho.prepare();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ReportEvent getReport() {
    return ReportEvent.createLegacyHtmlReport(name, chartgen.generate(histo)
        + "<pre>" + histo + "</pre>");
  }

  public Histogram<String> getHistogram() {
    return histo;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: multigrepspec(name, grepspecfile)");

        String name = argv[0];
        String fname = argv[1];
        MultiGrepReporterBuilder mgrb =
            new MultiGrepReporterBuilder(name, fname);
        Collection<MultiGrepReporterSink<String>> sinks;
        try {
          sinks = mgrb.load();
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to create multigrep report named " + name
                  + " with spec from file " + fname + ": " + e);
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
        Preconditions.checkArgument(argv.length >= 2,
            "usage: multigrep(name, str1[,str2...])");

        String name = argv[0];
        String[] strings = Arrays.copyOfRange(argv, 1, argv.length);
        EventSink snk = MultiGrepReporterSink.build(name, strings);
        return snk;
      }
    };
  }
}
