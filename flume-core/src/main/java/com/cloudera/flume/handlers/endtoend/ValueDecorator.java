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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This decorator adds values to events that pass through it.
 * By default decorator does not escape value, use "escape=true" to escape it.
 */
public class ValueDecorator<S extends EventSink> extends EventSinkDecorator<S> {
  public static final String KW_ESCAPE = "escape";
  final String attr; // attribute to tag

  // We store the value in two forms for optimization purposes
  final String unescapedValue;
  final byte[] value;

  final boolean escape;

  public ValueDecorator(S s, String attr, String value) {
    this(s, attr, value, false);
  }

  public ValueDecorator(S s, String attr, String value, boolean escape) {
    super(s);
    this.attr = attr;
    this.escape = escape;
    this.unescapedValue = value;
    this.value = value.getBytes().clone();
  }

  public void append(Event e) throws IOException, InterruptedException {
    byte[] attrVal = escape ? e.escapeString(unescapedValue).getBytes() : value;
    e.set(attr, attrVal);
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: value(\"attr\", \"value\"{, " + KW_ESCAPE + "=true|false})");
        String attr = argv[0];
        String v = argv[1];
        String escapedArg = context.getValue(KW_ESCAPE);
        boolean escape = (escapedArg == null) ? false : Boolean.parseBoolean(escapedArg);
        return new ValueDecorator<EventSink>(null, attr, v, escape);
      }
    };

  }
}
