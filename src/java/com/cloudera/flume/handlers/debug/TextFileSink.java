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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.DebugOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This writes data out to a textfile. Data is written in the format that is
 * specified. If there no/null format is specified, the default toString format
 * is used.
 */
public class TextFileSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(TextFileSink.class);

  String fname = null;
  OutputStream out = null;
  final OutputFormat fmt;
  long count = 0;

  public TextFileSink(String fname) {
    this(fname, null);
  }

  public TextFileSink(String fname, OutputFormat fmt) {
    this.fname = fname;
    this.fmt = (fmt == null) ? new RawOutputFormat() : fmt;
  }

  @Override
  synchronized public void append(Event e) throws IOException,
      InterruptedException {
    fmt.format(out, e);
    out.flush();
    count++;
    super.append(e);
  }

  @Override
  synchronized public void close() throws IOException {
    if (out != null) {
      out.close();
      out = null;
    }
  }

  @Override
  synchronized public void open() throws IOException {
    Preconditions.checkState(out == null, "double open not permitted");
    File f = new File(fname);
    out = new FileOutputStream(f);
  }

  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setLongMetric(ReportEvent.A_COUNT, count);
    return rpt;
  }

  @Deprecated
  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(ReportEvent.A_COUNT, count);
    return rpt;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        Preconditions.checkArgument(args.length >= 1 && args.length <= 2,
            "usage: text(filename[,format])");
        OutputFormat fmt = DebugOutputFormat.builder().build();
        String val = null;
        if (args.length >= 2) {
          val = args[1];
        } else {
          val = context.getValue("format");
        }

        if (val != null) {
          try {
            fmt = FormatFactory.get().getOutputFormat(val);
          } catch (FlumeSpecException e) {
            LOG.error("Illegal output format " + args[1], e);
            throw new IllegalArgumentException("Illegal output format" + val);
          }
        }
        return new TextFileSink(args[0], fmt);
      }
    };
  }

  public OutputFormat getFormat() {
    return fmt;
  }
}
