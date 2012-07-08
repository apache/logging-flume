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
import java.util.Map.Entry;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.google.common.base.Preconditions;

/**
 * Simple useful class - replace the body of an event with a formatted string.
 * e.g. format("Number of requests: %b")
 * 
 * Useful in particular for low-volume alerting / messaging.
 */
public class FormatterDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  final String formatString;

  public FormatterDecorator(S s, String formatString) {
    super(s);
    Preconditions.checkNotNull(formatString);
    this.formatString = formatString;
  }

  /**
   * Replaces e with an event whose body is formatString substituted with e's
   * body
   */
  public void append(Event e) throws IOException, InterruptedException {
    Preconditions.checkNotNull(e);
    String body = e.escapeString(formatString);
    Event e2 = new EventImpl(body.getBytes(), e.getTimestamp(),
        e.getPriority(), e.getNanos(), e.getHost());
    for (Entry<String, byte[]> entry : e.getAttrs().entrySet()) {
      e2.set(entry.getKey(), entry.getValue());
    }
    super.append(e2);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 1, "usage: format(pattern)");
        return new FormatterDecorator<EventSink>(null, argv[0]);
      }
    };
  }
}
