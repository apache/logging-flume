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
package com.cloudera.flume.reporter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.FanOutSink;

/**
 * This class extends the MultiSink to require ReportingSinks. This allows adds
 * methods to generate many reports with a single call.
 */
public class MultiReporter extends FanOutSink<EventSink> {
  String name;

  public MultiReporter(String name) {
    super();
    this.name = name;
  }

  public MultiReporter(String name, Collection<? extends EventSink> l) {
    super(l);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ReportEvent getReport() {
    StringWriter baos = new StringWriter();
    try {
      for (EventSink r : iter()) {
        r.getReport().toText(baos);
        baos.write('\n');
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ReportEvent.createLegacyHtmlReport(name, baos.toString());
  }
}
