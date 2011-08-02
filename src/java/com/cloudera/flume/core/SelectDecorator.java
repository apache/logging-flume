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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.google.common.base.Preconditions;

/**
 * This decorator 'selects' attributes (in the sql sense) from an event and
 * propagates only the attributes specified. This corresponds to the projection
 * operation in relational algebra.
 */
public class SelectDecorator<S extends EventSink> extends EventSinkDecorator<S> {
  String[] attrs;

  public SelectDecorator(S s, String... attrs) {
    super(s);
    this.attrs = attrs;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    Event e2 = EventImpl.select(e, attrs);
    super.append(e2);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length >= 1,
            "usage: select(attr1,...)");
        return new SelectDecorator<EventSink>(null, argv);
      }

    };
  }

}
