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
import com.google.common.base.Preconditions;

/**
 * Simple print to console. This defaults to the "debug" output format instead
 * of raw to give interactive users more feedback and information.
 */
public class ConsoleEventSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(ConsoleEventSink.class);

  OutputFormat fmt;

  public ConsoleEventSink() {
    this(null);
  }

  public ConsoleEventSink(OutputFormat fmt) {
    this.fmt = (fmt == null) ? DebugOutputFormat.builder().build() : fmt;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    fmt.format(System.out, e);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
    LOG.info("ConsoleEventSink( " + fmt.getBuilder().getName() + " ) closed");
  }

  @Override
  public void open() throws IOException {
    LOG.info("ConsoleEventSink( " + fmt.getBuilder().getName() + " ) opened");
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 1,
            "usage: console[(format)]");
        OutputFormat fmt = DebugOutputFormat.builder().build();
        if (argv.length >= 1) {
          // TODO (jon) handle formats with arguments. Requires language update.
          try {
            fmt = FormatFactory.get().getOutputFormat(argv[0]);
          } catch (FlumeSpecException e) {
            LOG.error("Bad output format name " + argv[0], e);
            throw new IllegalArgumentException("Bad output format name "
                + argv[0], e);
          }
        }
        return new ConsoleEventSink(fmt);
      }
    };
  }
}
