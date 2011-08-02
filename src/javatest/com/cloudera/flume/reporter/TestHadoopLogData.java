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

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.reporter.builder.SimpleRegexReporterBuilder;
import com.cloudera.flume.reporter.histogram.RegexGroupHistogramSink;

/**
 * This loads multiple regex histogram reporters
 * 
 * Currently this is just a quick test harness to make sure things don't
 * explode, and to feel out the data set that I am looking at.
 */
public class TestHadoopLogData implements ExampleData {

  @Test
  public void testLineCount() throws IOException, InterruptedException {
    EventSource src = new NoNlASCIISynthSource(25, 100, 1);
    src.open();

    SimpleRegexReporterBuilder b = new SimpleRegexReporterBuilder(
        HADOOP_REGEXES);
    Collection<RegexGroupHistogramSink> sinks = b.load();
    MultiReporter mr = new MultiReporter("apache_sinks", sinks);
    mr.open();

    EventUtil.dumpAll(src, mr);

    for (RegexGroupHistogramSink r : sinks) {
      System.out.println(r.getHistogram());
    }
    Assert.assertEquals(5, sinks.size());

  }

}
