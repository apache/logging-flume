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

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This test sink and source generating factories.
 */
public class TestFactories extends TestCase implements ExampleData {
  public static SinkFactory fact = new SinkFactoryImpl();
  public static SourceFactory srcfact = new SourceFactoryImpl();

  final static int LINES = 25;

  public void testBuildConsole() throws IOException, FlumeSpecException {
    EventSink snk = fact.getSink(new Context(), "console");
    snk.open();
    snk.append(new EventImpl("test".getBytes()));
    snk.close();
  }

  public void testBuildTextSource() throws IOException, FlumeSpecException {
    // 25 lines of 100 bytes of ascii
    EventSource src = srcfact.getSource("asciisynth", "25", "100");
    src.open();
    Event e = null;
    int cnt = 0;
    while ((e = src.next()) != null) {
      System.out.println(e);
      cnt++;
    }
    src.close();
    assertEquals(LINES, cnt);
  }

  public void testConnector() throws IOException, InterruptedException,
      FlumeSpecException {
    EventSink snk = fact.getSink(new Context(), "console");
    snk.open();

    EventSource src = srcfact.getSource("asciisynth", "25", "100");
    src.open();

    DirectDriver conn = new DirectDriver(src, snk);
    conn.start();

    conn.join(Long.MAX_VALUE);

    snk.close();
    src.close();
  }

  public void testDecorator() throws IOException, FlumeSpecException {
    EventSource src = srcfact.getSource("asciisynth", "25", "100");
    src.open();

    EventSinkDecorator<EventSink> deco =

    fact.getDecorator(new Context(), "intervalSampler", "5");
    EventSink snk = fact.getSink(new Context(), "counter", "name");

    snk.open();

    deco.setSink(snk);
    EventUtil.dumpAll(src, snk);
  }

  /**
   * This seems to fail about 1 out of 10 times. There is a timing issues due to
   * the multi-threading.
   */
  public void testThrift() throws IOException, InterruptedException,
      FlumeSpecException {
    System.out
        .println("Testing a more complicated pipeline with a thrift network connection in the middle");
    EventSource tsrc = srcfact.getSource("rpcSource", "31337");
    EventSink tsnk = fact.getSink(new Context(), "rpcSink", "0.0.0.0", "31337");
    // thrift src needs to be started before the thrift sink can connect to it.
    tsrc.open();
    tsnk.open();

    Thread.sleep(100); // need some time to open the connector.

    EventSink counter = fact.getSink(new Context(), "counter", "count");
    EventSource txtsrc = srcfact.getSource("asciisynth", "25", "100");
    counter.open();
    txtsrc.open();

    DirectDriver svrconn = new DirectDriver(tsrc, counter);
    svrconn.start();

    DirectDriver cliconn = new DirectDriver(txtsrc, tsnk);
    cliconn.start();

    cliconn.join(Long.MAX_VALUE);
    Thread.sleep(250);

    svrconn.stop();
    tsnk.close();
    tsrc.close();

    counter.close();
    txtsrc.close();

    System.out.println("read " + ((CounterSink) counter).getCount() + " lines");
    assertEquals(LINES, ((CounterSink) counter).getCount());
    assertNull(cliconn.getError());
    assertNull(svrconn.getError());

  }
}
