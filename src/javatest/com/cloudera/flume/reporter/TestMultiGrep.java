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

import junit.framework.TestCase;

import org.arabidopsis.ahocorasick.AhoCorasick;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.reporter.builder.MultiGrepReporterBuilder;
import com.cloudera.flume.reporter.histogram.MultiGrepReporterSink;
import com.cloudera.util.Histogram;

public class TestMultiGrep extends TestCase implements ExampleData {
  final static String NPE = "NullPointerException";
  final static String LOST = "Lost tracker";

  final static String[] searches = { NPE, "ConnectException",
      "Retrying connect to server", "Receiving block", "Received block", LOST,
      "mapred.TaskTracker: Resending" };

  public void testMultiGrep() throws IOException {
    // build the aho from the file.
    AhoCorasick<String> aho = new AhoCorasick<String>();
    for (String s : searches) {
      aho.add(s.getBytes(), s);
    }

    MultiGrepReporterSink<String> snk = new MultiGrepReporterSink<String>(
        "multi grep", aho);
    snk.open();

    EventSource src = new NoNlASCIISynthSource(25, 100, 1);
    src.open();

    EventUtil.dumpAll(src, snk);

    snk.append(new EventImpl(NPE.getBytes()));
    snk.append(new EventImpl(LOST.getBytes()));
    snk.append(new EventImpl(LOST.getBytes()));

    Histogram<String> h = snk.getHistogram();
    System.out.println(h);

    assertEquals(3, h.total());
    assertEquals(2, h.get(LOST));
    assertEquals(1, h.get(NPE));

    snk.close();
    src.close();

  }

  public void testMultiGrepBuilder() throws IOException {
    Collection<MultiGrepReporterSink<String>> c = new MultiGrepReporterBuilder(
        HADOOP_GREP).load();
    assertEquals(1, c.size());

    MultiGrepReporterSink<String> snk = c.iterator().next();
    snk.open();

    EventSource src = new NoNlASCIISynthSource(25, 100, 1);

    src.open();

    EventUtil.dumpAll(src, snk);
    snk.append(new EventImpl(NPE.getBytes()));
    snk.append(new EventImpl(LOST.getBytes()));
    snk.append(new EventImpl(LOST.getBytes()));

    Histogram<String> h = snk.getHistogram();
    System.out.println(h);

    assertEquals(3, h.total());
    assertEquals(2, h.get(LOST));
    assertEquals(1, h.get(NPE));

    snk.close();
    src.close();

  }
}
