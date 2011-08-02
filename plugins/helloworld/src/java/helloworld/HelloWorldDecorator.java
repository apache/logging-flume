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
package helloworld;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Simple Decorator that prepends "Hello World!" to every event body.
 */
public class HelloWorldDecorator<S extends EventSink> extends EventSinkDecorator<S> {
  public HelloWorldDecorator(S s) {
    super(s);
  }

  @Override
  public void append(Event e) throws IOException {
    String helloWorldBody = "Hello World! -- " + new String(e.getBody());

    // make a copy of the event, but with the new body string.
    EventImpl e2 = new EventImpl(helloWorldBody.getBytes(),
        e.getTimestamp(), e.getPriority(), e.getNanos(), e.getHost(),
        e.getAttrs());

    super.append(e2);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      // construct a new parameterized decorator
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0,
            "usage: helloWorldDecorator");

        return new HelloWorldDecorator<EventSink>(null);
      }

    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin decorator.
   */
  public static List<Pair<String, SinkDecoBuilder>> getDecoratorBuilders() {
    List<Pair<String, SinkDecoBuilder>> builders =
      new ArrayList<Pair<String, SinkDecoBuilder>>();
    builders.add(new Pair<String, SinkDecoBuilder>("helloWorldDecorator",
        builder()));
    return builders;
  }
}
