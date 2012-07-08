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
import com.cloudera.flume.handlers.batch.BatchingDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.thrift.ThriftEventSink;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.cloudera.util.Benchmark;

/**
 * These tests are for microbenchmarking the thrift sink and server elements.
 */
public class PerfThriftSinks {

  /**
   * mem -> ThriftEventSink -> ThriftEventSource -> NullSink
   */
  @Test
  public void testThriftSend() throws IOException, InterruptedException {

    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0",
        conf.getCollectorPort());
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");
    // MB/s = B/us
    b.mark("MB/s", (double) snk.getSentBytes()
        / (double) (b.getLastDelta() / 1000));
    tes.close();
    snk.close();
    drain.interrupt();
    b.done();
  }

  /**
   * mem -> batch(10) ThriftEventSink -> ThriftEventSource -> NullSink
   */
  @Test
  public void testThriftBatchSend10() throws IOException, InterruptedException {

    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink tsnk = new ThriftEventSink("0.0.0.0",
        conf.getCollectorPort());
    // make size happen first all the time.
    final BatchingDecorator snk = new BatchingDecorator(tsnk, 10, 10000000);
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");
    // MB/s = B/us
    b.mark("MB/s", (double) tsnk.getSentBytes()
        / (double) (b.getLastDelta() / 1000));

    tes.close();
    snk.close();
    drain.interrupt();
    b.done();
  }

  /**
   * mem -> batch(10) ThriftEventSink -> ThriftEventSource -> NullSink
   */
  @Test
  public void testThriftBatchSend100() throws IOException, InterruptedException {

    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink tsnk = new ThriftEventSink("0.0.0.0",
        conf.getCollectorPort());
    // make size happen first all the time.
    final BatchingDecorator snk = new BatchingDecorator(tsnk, 100, 10000000);
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");
    // MB/s = B/us
    b.mark("MB/s", (double) tsnk.getSentBytes()
        / (double) (b.getLastDelta() / 1000));

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

    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0",
        conf.getCollectorPort());

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
    // MB/s = B/us
    b.mark("MB/s", (double) snk.getSentBytes()
        / (double) (b.getLastDelta() / 1000));

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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testThriftRawSend() throws IOException, InterruptedException {

    Benchmark b = new Benchmark("nullsink");
    b.mark("begin");

    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    final ThriftEventSource tes = new ThriftEventSource(conf.getCollectorPort());
    tes.open();
    // need to drain the sink otherwise its queue will fill up with events!
    Thread drain = new Thread("drain") {
      public void run() {
        try {
          EventUtil.dumpAll(tes, new NullSink());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    drain.start(); // drain the sink.
    b.mark("receiver_started");

    final ThriftEventSink snk = new ThriftEventSink("0.0.0.0",
        conf.getCollectorPort());
    snk.open();
    b.mark("sink_started");

    EventUtil.dumpAll(mem, snk);
    b.mark("thrift sink to thrift source done");
    // MB/s = B/us
    b.mark("MB/s", (double) snk.getSentBytes()
        / (double) (b.getLastDelta() / 1000));

    tes.close();
    snk.close();
    drain.interrupt();
    b.done();
  }

}
