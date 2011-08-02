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
 */
public class ValueDecorator<S extends EventSink> extends EventSinkDecorator<S> {

  final String attr; // attribute to tag
  final byte[] value;

  public ValueDecorator(S s, String attr, byte[] value) {
    super(s);
    this.attr = attr;
    this.value = value.clone();
  }

  public void append(Event e) throws IOException {
    e.set(attr, value);
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: value(\"attr\", \"value\")");
        String attr = argv[0];
        String v = argv[1];
        return new ValueDecorator<EventSink>(null, attr, v.getBytes());
      }
    };

  }
}
