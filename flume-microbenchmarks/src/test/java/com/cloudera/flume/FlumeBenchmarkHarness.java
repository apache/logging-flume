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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.AttrSynthSource;
import com.cloudera.flume.handlers.debug.BenchmarkReportDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.FlumeTestHarness;

public class FlumeBenchmarkHarness extends FlumeTestHarness {
  // This is a tiny test set, suitable for step-through debugging
  @SuppressWarnings("serial")
  public static Map<String, EventSource> createTinyCases() {
    return new HashMap<String, EventSource>() {
      {
        // datasets with fields, x attributes, 10 byte long attr names, 10
        // byte values.
        put("10,10,5,5,8", new AttrSynthSource(10, 5, 5, 8, 1337));
      }
    };
  }

  // This is a data set that varies the size of the body of an event.
  @SuppressWarnings("serial")
  public static Map<String, EventSource> createVariedMsgBytesCases() {
    return new HashMap<String, EventSource>() {
      {
        // 1337 is the rand seed.

        // this is *really* slow
        put("100000,10,0,0,0", new NoNlASCIISynthSource(100000, 10, 1337));
        put("100000,100,0,0,0", new NoNlASCIISynthSource(100000, 100, 1337));
        put("100000,1000,0,0,0", new NoNlASCIISynthSource(100000, 1000, 1337));
        put("100000,3000,0,0,0", new NoNlASCIISynthSource(100000, 3000, 1337));
        put("100000,10000,0,0,0", new NoNlASCIISynthSource(100000, 10000, 1337));
      }
    };
  }

  // This dataset varies the # of attributes an event has. The latter two
  // entries send fewer messages because the size of the messages are memory
  // prohibitive
  @SuppressWarnings("serial")
  public static Map<String, EventSource> createVariedNumAttrsCases() {
    return new HashMap<String, EventSource>() {
      {
        // datasets with fields, x attributes, 10 byte long attr names, 10
        // byte values.
        put("100000,0,10,10,10", new AttrSynthSource(100000, 10, 10, 10, 1337));
        put("100000,0,100,10,10",
            new AttrSynthSource(100000, 100, 10, 10, 1337));
        put("10000,0,1000,10,10",
            new AttrSynthSource(10000, 1000, 10, 10, 1337));
        put("1000,0,10000,10,10",
            new AttrSynthSource(1000, 10000, 10, 10, 1337));
      }
    };
  }

  // This dataset varies the size of the values associated with an attribute.
  @SuppressWarnings("serial")
  public static Map<String, EventSource> createVariedValSizeCases() {
    return new HashMap<String, EventSource>() {
      {
        // datasets with fields, 10 attributes, 10 byte long attr names, xx
        // byte values.
        put("100000,0,10,10,10", new AttrSynthSource(100000, 10, 10, 10, 1337));
        put("100000,0,10,10,100",
            new AttrSynthSource(100000, 10, 10, 100, 1337));
        put("100000,0,10,10,1000", new AttrSynthSource(100000, 10, 10, 1000,
            1337));
        put("1000,0,10,10,10000", new AttrSynthSource(10000, 10, 10, 10000,
            1337));
      }
    };
  }

  /**
   * This takes what ever data set comes in and multiplies it by 10x volume.
   */
  public static EventSink createDecoratorBenchmarkSink(String name, String deco)
      throws FlumeSpecException {
    String spec = "mult(10) benchinject " + deco + " benchreport(\"" + name
        + "\") null";
    return FlumeBuilder.buildSink(new Context(), spec);
  }

  public static EventSink createSinkBenchmark(String name, String sink)
      throws FlumeSpecException {
    String spec = "benchinject benchreport(\"" + name + "\") " + sink;
    return FlumeBuilder.buildSink(new Context(), spec);
  }

  /**
   * This takes a single decorator, and then applies all of the datasets through
   * the decorator. Each source is bufferzied -- the given number of messages
   * are stored in memory so that they can be blasted through any deco.
   * 
   * @throws InterruptedException
   */
  public static void doDecoBenchmark(String deco, Map<String, EventSource> sets)
      throws FlumeSpecException, IOException, InterruptedException {
    for (Map.Entry<String, EventSource> ent : sets.entrySet()) {
      setupLocalWriteDir();
      ReportManager.get().clear();

      // copy all events into memory
      EventSource src = MemorySinkSource.bufferize(ent.getValue());
      EventSink snk = createDecoratorBenchmarkSink(ent.getKey() + "," + deco,
          deco);
      src.open();
      snk.open();
      EventUtil.dumpAll(src, snk);
      src.close();
      snk.close();
      dumpReports();
      cleanupLocalWriteDir();
    }
  }

  /**
   * This gets reports and outputs them to std err in csv format.
   */
  public static void dumpReports() {
    ReportManager rman = ReportManager.get();
    SortedMap<String, Reportable> sorted = new TreeMap<String, Reportable>(
        rman.getReportables());
    for (Map.Entry<String, Reportable> ent : sorted.entrySet()) {
      String params = ent.getKey();
      ReportEvent r = ent.getValue().getMetrics();
      System.out.println(new String(r.toString()));
      System.err.print(new Date(r.getTimestamp()) + ",");
      System.err.print(params + ",");
      System.err.print(Attributes.readString(r,
          BenchmarkReportDecorator.A_BENCHMARK_CSV));
    }
  }

  /**
   * This is about 300MB in memory
   */
  public static MemorySinkSource synthInMem() throws IOException,
      InterruptedException {
    return synthInMem(1000000, 100, 1);
  }

  public static MemorySinkSource synthInMem(int count, int bodySz, int seed)
      throws IOException, InterruptedException {
    EventSource txt = new NoNlASCIISynthSource(count, bodySz, seed);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    return mem;
  }

}
