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
package com.cloudera.flume.reporter.history;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.charts.ChartPackage;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This counts elements that are appended in each epoch.
 */
public class CountHistoryReporter extends ScheduledHistoryReporter<CounterSink> {

  public CountHistoryReporter(String name, long maxAge) {
    this(name, maxAge, new DumbTagger());
  }

  public CountHistoryReporter(String name, long maxAge, Tagger t) {
    super(name, maxAge, FlumeConfiguration.get().getHistoryMaxLength(), t);
  }

  @Override
  public CounterSink newSink(Tagger format) throws IOException {
    return new CounterSink(format.newTag());
  }

  @Override
  public ReportEvent getReport() {

    ArrayList<Pair<Long, Long>> list =
        new ArrayList<Pair<Long, Long>>(getHistory().size());
    for (Pair<Long, CounterSink> p : getHistory()) {
      list.add(new Pair<Long, Long>(p.getLeft(), p.getRight().getCount()));
    }
    String report = ChartPackage.createTimelineGen().generate(list);

    return ReportEvent.createLegacyHtmlReport(name, report);
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 2,
            "usage: counterHistory(name[, period_ms])");
        String name = argv[0];
        long period = FlumeConfiguration.get().getHistoryDefaultPeriod();
        if (argv.length == 2) {
          period = Integer.parseInt(argv[1]);
        }
        EventSink snk = new CountHistoryReporter(name, period);
        return snk;
      }
    };
  }

}
