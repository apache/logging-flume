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
package com.cloudera.flume;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.handlers.thrift.ThriftEventSink;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.cloudera.util.Benchmark;

/**
 * These tests are for microbenchmarking the thrift sink and server elements.
 */
public class PerfThriftSinks implements ExamplePerfData {

  /**
   * Pipeline is:
   * 
   * text -> mem
   * 
   * mem -> ThriftEventSink -> ThriftEventSource -> NullSink
   */
  @Test
  public void testThriftSend() throws IOException {

    Benchmark b = new Benchmark("nullsink");

    b.mark("begin");

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0", conf
        .getCollectorPort());
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");

    tes.close();
    snk.close();
    drain.interrupt();
    b.done();
  }

  /**
   * This is slighlty different by using another thread to kick off the sink. It
   * shouldn't really matter much.
   * 
   * Pipeline is:
   * 
   * text -> mem
   * 
   * mem -> ThriftEventSink -> ThriftEventSource -> NullSink
   **/
  @Test
  public void testThriftSendMulti() throws IOException, InterruptedException {

    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0", conf
        .getCollectorPort());

    Thread t = new Thread() {
      public void run() {
        try {
          snk.open();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    t.start();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");

    Thread.sleep(1000);
    tes.close();
    snk.close();
    t.interrupt();
    drain.interrupt();
    b.done();
  }

  /**
   * Here we are using the ThriftRawEventSink instead of the ThriftEventSink
   * 
   * Pipeline is:
   * 
   * text -> mem
   * 
   * mem -> ThriftRawEventSink -> ThriftEventSource -> NullSink
   */
  @Test
  public void testThriftRawSend() throws IOException {

    Benchmark b = new Benchmark("nullsink");

    b.mark("begin");

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0", conf
        .getCollectorPort());
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");

    tes.close();
    snk.close();
    drain.interrupt();
    b.done();
  }

}
