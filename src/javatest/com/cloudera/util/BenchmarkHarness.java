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
package com.cloudera.util;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LivenessManager;
import com.cloudera.flume.agent.LogicalNodeManager;
import com.cloudera.flume.agent.MockMasterRPC;
import com.cloudera.flume.agent.diskfailover.DiskFailoverManager;
import com.cloudera.flume.agent.diskfailover.NaiveFileFailoverManager;
import com.cloudera.flume.agent.durability.NaiveFileWALManager;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.AttrSynthSource;
import com.cloudera.flume.handlers.debug.BenchmarkReportDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;

/**
 * This sets up a batttery of synthetic datasets for testing against different
 * decorators and sinks. Generally, each test requires ~2GB mem. ~1GB for
 * keeping a data set in memory and the other GB for some gc headroom.
 */
@SuppressWarnings("serial")
public class BenchmarkHarness {
  final static Logger LOG = Logger.getLogger(BenchmarkHarness.class.getName());

  // These are setup to point to new default logging dir for each test.
  public static FlumeNode node;
  public static MockMasterRPC mock;
  public static File tmpdir;

  /**
   * This sets the log dir in the FlumeConfiguration and then instantiates a
   * mock master and node that use that configuration
   */
  public static void setupLocalWriteDir() {
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      Assert.fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.clear(); // reset all back to defaults.
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    mock = new MockMasterRPC();
    node = new FlumeNode(mock, false /* starthttp */, false /* oneshot */);
    ReportManager.get().clear();
  }

  /**
   * This version allows a particular test case to replace the default
   * xxxManager with one that is reasonable for the test.
   * 
   * Any args that are null will default to the "normal" version.
   */
  public static void setupFlumeNode(LogicalNodeManager nodesMan,
      WALManager walMan, DiskFailoverManager dfMan,
      CollectorAckListener colAck, LivenessManager liveman) {
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      Assert.fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    mock = new MockMasterRPC();

    nodesMan = (nodesMan != null) ? nodesMan : new LogicalNodeManager(NetUtils
        .localhost());
    walMan = (walMan != null) ? walMan : new NaiveFileWALManager(new File(conf
        .getAgentLogsDir()));
    dfMan = (dfMan != null) ? dfMan : new NaiveFileFailoverManager(new File(
        conf.getAgentLogsDir()));
    colAck = (colAck != null) ? colAck : new CollectorAckListener(mock);
    liveman = (liveman != null) ? liveman : new LivenessManager(nodesMan, mock,
        walMan);

    node = new FlumeNode(NetUtils.localhost(), mock, nodesMan, walMan, dfMan,
        colAck, liveman);
  }

  /**
   * Cleanup the temp dir after the test is run.
   */
  public static void cleanupLocalWriteDir() throws IOException {
    FileUtil.rmr(tmpdir);
  }

  // This is a tiny test set, suitable for step-through debugging
  public static Map<String, EventSource> tiny = new HashMap<String, EventSource>() {
    {
      // datasets with fields, x attributes, 10 byte long attr names, 10
      // byte
      // values.
      put("10,10,5,5,8", new AttrSynthSource(10, 5, 5, 8, 1337));
    }
  };

  // This is a data set that varies the size of the body of an event.
  public static Map<String, EventSource> varyMsgBytes = new HashMap<String, EventSource>() {
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

  // This dataset varies the # of attributes an event has. The latter two
  // entries send fewer messages because the size of the messages are memory
  // prohibitive
  public static Map<String, EventSource> varyNumAttrs = new HashMap<String, EventSource>() {
    {
      // datasets with fields, x attributes, 10 byte long attr names, 10
      // byte
      // values.
      put("100000,0,10,10,10", new AttrSynthSource(100000, 10, 10, 10, 1337));
      put("100000,0,100,10,10", new AttrSynthSource(100000, 100, 10, 10, 1337));
      put("10000,0,1000,10,10", new AttrSynthSource(10000, 1000, 10, 10, 1337));
      put("1000,0,10000,10,10", new AttrSynthSource(1000, 10000, 10, 10, 1337));
    }
  };

  // This dataset varies the size of the values associated with an attribute.
  public static Map<String, EventSource> varyValSize = new HashMap<String, EventSource>() {
    {
      // datasets with fields, 10 attributes, 10 byte long attr names, xx
      // byte
      // values.
      put("100000,0,10,10,10", new AttrSynthSource(100000, 10, 10, 10, 1337));
      put("100000,0,10,10,100", new AttrSynthSource(100000, 10, 10, 100, 1337));
      put("100000,0,10,10,1000",
          new AttrSynthSource(100000, 10, 10, 1000, 1337));
      put("1000,0,10,10,10000", new AttrSynthSource(10000, 10, 10, 10000, 1337));
    }
  };

  /**
   * This takes what ever data set comes in and multiplies it by 10x volume.
   */
  public static EventSink createDecoratorBenchmarkSink(String name, String deco)
      throws FlumeSpecException {
    String spec = "let benchsink := { benchreport(\"" + name
        + "\") => null } in { mult(10) => { benchinject => { " + deco
        + " => benchsink } } }";

    return FlumeBuilder.buildSink(new Context(), spec);
  }

  public static EventSink createSinkBenchmark(String name, String sink)
      throws FlumeSpecException {
    String spec = "{benchinject => {benchreport(\"" + name + "\") => " + sink
        + " } }";
    return FlumeBuilder.buildSink(new Context(), spec);
  }

  /**
   * This takes a single decorator, and then applies all of the datasets through
   * the decorator. Each source is bufferzied -- the given number of messages
   * are stored in memory so that they can be blasted through any deco.
   */
  public static void doDecoBenchmark(String deco, Map<String, EventSource> sets)
      throws FlumeSpecException, IOException {
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
    SortedMap<String, Reportable> sorted = new TreeMap<String, Reportable>(rman
        .getReportables());
    for (Map.Entry<String, Reportable> ent : sorted.entrySet()) {
      String params = ent.getKey();
      ReportEvent r = ent.getValue().getReport();
      System.out.println(new String(r.toString()));
      System.err.print(new Date(r.getTimestamp()) + ",");
      System.err.print(params + ",");
      System.err.print(Attributes.readString(r,
          BenchmarkReportDecorator.A_BENCHMARK_CSV));
    }
  }
}
