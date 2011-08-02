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
package com.cloudera.flume.core;

import java.io.IOException;
import java.util.Map;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This sink takes a data flow spec string as a constructor argument. It parses
 * the construction and instantiates the sink. During execution, all the sink
 * API calls are forwarded to the instantiated sink.
 * 
 * Currently, there no builder method because this should only be used by
 * sinks/decorators that need to embed sinks specs as arguments. In the future
 * there may be due to different spec syntax.
 */
public class CompositeSink extends EventSink.Base {
  final EventSink snk;

  public CompositeSink(Context context, String spec) throws FlumeSpecException {
    snk = FlumeBuilder.buildSink(context, spec);
    if (snk == null) {
      throw new FlumeSpecException(
          "Illegal flume spec expression (sink/source/deco not found)");
    }
  }

  @Override
  public void open() throws IOException, InterruptedException {
    snk.open();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    snk.close();
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    snk.append(e);
    super.append(e);
  }

  @Override
  public String getName() {
    return snk.getName();
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    snk.getReports(namePrefix + getName() + ".", reports);
  }

}
