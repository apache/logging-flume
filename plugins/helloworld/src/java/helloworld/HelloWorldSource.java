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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * Simple Source that generates a "hello world!" event every 3 seconds.
 */
public class HelloWorldSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(HelloWorldSource.class);
  
  private String helloWorld;

  @Override
  public void open() throws IOException {
    // Initialized the source
    helloWorld = "Hello World!";
  }

  @Override
  public Event next() throws IOException {
    // Next returns the next event, blocking if none available.
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage(), e);
    }
    return new EventImpl(helloWorld.getBytes());
  }

  @Override
  public void close() throws IOException {
    // Cleanup
    helloWorld = null;
  }

  public static SourceBuilder builder() {
    // construct a new parameterized source
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx,String... argv) {
        Preconditions.checkArgument(argv.length == 0,
            "usage: helloWorldSource");

        return new HelloWorldSource();
      }
    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin source.
   */
  public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceBuilder>> builders =
      new ArrayList<Pair<String, SourceBuilder>>();
    builders.add(new Pair<String, SourceBuilder>("helloWorldSource", builder()));
    return builders;
  }
}
