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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.util.Benchmark;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This extracts benchmark events inserted by the BenchmarkInjectDecorator.
 * There should be a start event followed by many normal events, and eventually
 * ending with an error or done event.
 * 
 * This assumes that event order is respected. This isn't exactly a safe
 * assumption once we go multithreaded or deal with errors, but this should be
 * sufficient for perftesting.
 */
public class BenchmarkReportDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {
  static final Logger LOG = LoggerFactory
      .getLogger(BenchmarkReportDecorator.class);

  public final static String A_BENCHMARK_RPT = "benchmarkReport";
  public final static String A_BENCHMARK_CSV = "benchmarkCsv";

  final String name;
  final EventSink reportSink;

  Map<String, Pair<Benchmark, StringWriter>> benchmarks = new HashMap<String, Pair<Benchmark, StringWriter>>();

  public BenchmarkReportDecorator(String name, S s) {
    super(s);
    this.name = name;
    this.reportSink = new NullSink();
  }

  public BenchmarkReportDecorator(String name, S s, S rpt) {
    super(s);
    Preconditions.checkArgument(rpt != null);
    this.name = name;
    this.reportSink = rpt;
  }

  /**
   * Checks for Benchmark Tags. If there are not tags events are passed through.
   * If ther are, there are three kinds - 'start' which instantiates a
   * benchmark; 'first' which starts a benchmark; and 'stop' which ends a
   * benchmark. These are consumed by this decorator.
   */
  @Override
  public void append(Event e) throws IOException, InterruptedException {
    byte[] bench = e.get(BenchmarkInjectDecorator.ATTR_BENCHMARK);
    if (bench == null) {
      // This is the normal case -- a regular message
      getSink().append(e);
      return;
    }

    // All of these messages are silently consumed and not forwarded
    byte[] tagbytes = e.get(BenchmarkInjectDecorator.ATTR_BENCHMARK_TAG);
    Preconditions.checkNotNull(tagbytes);
    String tag = new String(tagbytes, CharEncUtils.RAW);

    if (Arrays.equals(bench, BenchmarkInjectDecorator.BENCH_START)) {
      StringWriter out = new StringWriter();
      PrintWriter pw = new PrintWriter(out);
      Benchmark b = new Benchmark(tag, pw, pw);
      b.mark("benchmarkStart");
      benchmarks.put(tag, new Pair<Benchmark, StringWriter>(b, out));
    } else if (Arrays.equals(bench, BenchmarkInjectDecorator.BENCH_FIRST)) {
      Benchmark b = benchmarks.get(tag).getLeft();
      b.mark("benchmarkFirst");
    } else if (Arrays.equals(bench, BenchmarkInjectDecorator.BENCH_STOP)) {
      Benchmark b = benchmarks.get(tag).getLeft();
      b.mark("benchmarkDone");
      b.done();

      ReportEvent rpt = getReport();
      LOG.info(rpt.toText());
      reportSink.append(rpt);

    } else if (Arrays.equals(bench, BenchmarkInjectDecorator.BENCH_ERROR)) {
      Benchmark b = benchmarks.get(tag).getLeft();
      b.mark("benchmarkError");
      b.done();
      LOG.info(getReport().toText());

      ReportEvent rpt = getReport();
      LOG.info(rpt.toText());
      reportSink.append(rpt);
    } else {
      String msg = "Unexpected Benchmark event type: " + tag;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

  }

  @Override
  public void open() throws IOException, InterruptedException {
    super.open();
    reportSink.open();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    super.close();
    reportSink.close();
    LOG.info(new String(getReport().getBody()));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());

    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    StringWriter csvsw = new StringWriter();
    PrintWriter csvpw = new PrintWriter(csvsw);
    for (Pair<Benchmark, StringWriter> e : benchmarks.values()) {
      out.print(e.getRight().getBuffer().toString());
      e.getLeft().printCsvLog(csvpw);
    }
    out.close();
    csvpw.close();

    Attributes.setString(rpt, A_BENCHMARK_RPT, sw.toString());
    Attributes.setString(rpt, A_BENCHMARK_CSV, csvsw.toString());
    return rpt;
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length >= 1 && argv.length <= 2,
            "usage: benchreport(name[,rptSink])");
        String name = argv[0];
        EventSink rptSink = new NullSink();
        if (argv.length >= 2) {
          String rptSpec = argv[1];
          try {
            rptSink = new CompositeSink(context, rptSpec);
          } catch (FlumeSpecException e) {
            LOG.debug("failed to parse rpt spec", e);
            throw new IllegalArgumentException(e.getMessage());
          }
        }

        BenchmarkReportDecorator<EventSink> bench = new BenchmarkReportDecorator<EventSink>(
            name, null, rptSink);

        ReportManager.get().add(bench);
        return bench;
      }

    };
  }
}
