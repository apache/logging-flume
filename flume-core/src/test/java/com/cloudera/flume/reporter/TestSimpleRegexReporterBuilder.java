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
import java.util.Collection;

import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.builder.SimpleRegexReporterBuilder;
import com.cloudera.flume.reporter.histogram.RegexGroupHistogramSink;

/**
 * This test the file loader on a known good file to make sure it doesn't
 * explode on us.
 */
public class TestSimpleRegexReporterBuilder implements ExampleData {

  public static final String sample = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] "
      + "\"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\""
      + " \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\"";

  /**
   * This is just to make sure it doesn't explode.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testLoad() throws IOException, InterruptedException {
    SimpleRegexReporterBuilder b = new SimpleRegexReporterBuilder(
        getClass().getClassLoader()
        .getResource(APACHE_REGEXES).getFile());

    Collection<RegexGroupHistogramSink> sinks = b.load();
    MultiReporter mr = new MultiReporter("apache_sinks", sinks);
    mr.open();
    mr.append(new EventImpl(sample.getBytes()));

    for (EventSink r : sinks) {
      System.out.println(r.getMetrics());
    }
  }
}
