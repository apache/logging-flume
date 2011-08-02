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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Simple Sink that writes to a "helloworld.txt" file.
 */
public class HelloWorldSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(HelloWorldSink.class);
  private PrintWriter pw;
  
  @Override
  public void open() throws IOException {
    // Initialized the sink
    pw = new PrintWriter(new FileWriter("helloworld.txt"));
  }

  @Override
  public void append(Event e) throws IOException {
    // append the event to the output
    
    // here we are assuming the body is a string
    pw.println(new String(e.getBody()));
    pw.flush(); // so we can see it in the file right away
  }

  @Override
  public void close() throws IOException {
    // Cleanup
    pw.flush();
    pw.close();
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      // construct a new parameterized sink
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 0,
            "usage: helloWorldSink");

        return new HelloWorldSink();
      }
    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin sink.
   */
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders =
      new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("helloWorldSink", builder()));
    return builders;
  }
}
