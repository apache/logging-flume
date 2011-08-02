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
package com.cloudera.flume.conf;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

/**
 * Tests the plugin loading mechanism for event sinks.
 * 
 * Java doesn't allow non top-level types to have static methods. So in order to
 * mock up a class to use to test, we have to make it top-level. Hence the test
 * class here acts as a plugin itself.
 */
public class TestPlugins extends EventSink.Base {
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders =
        new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("test-plugin",
        new SinkBuilder() {
          @Override
          public EventSink build(Context context, String... argv) {
            return new TestPlugins();
          }
        }));
    return builders;
  }

  /**
   * Instantiates a factory and checks to see if the class is successfully
   * loaded.
   */
  @Test
  public void testLoadPlugins() {
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.PLUGIN_CLASSES,
        "com.cloudera.flume.conf.TestPlugins,com.cloudera.nonexistant.Sink");
    SinkFactoryImpl factory = new SinkFactoryImpl();
    assertTrue(factory.getSinkNames().contains("test-plugin"));
  }

}
