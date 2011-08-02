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

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.reporter.builder.MultiGrepReporterBuilder;
import com.cloudera.flume.reporter.histogram.MultiGrepReporterSink;
import com.cloudera.util.Benchmark;
import com.cloudera.util.Histogram;

/**
 * This set of performance tests isolate the system from I/O so so we can
 * measure the overhead of the actual reporting machinery.
 * 
 * This crashes with OOME's .. What is wrong!?
 */
public class PerfGrepReportSinks implements ExamplePerfData {

  @Test
  public void testHadoopGrep() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("hadoop_greps");
    b.mark("begin");

    MultiGrepReporterBuilder bld = new MultiGrepReporterBuilder(HADOOP_GREP);

    MultiGrepReporterSink<String> snk = bld.load().iterator().next();
    snk.open();
    b.mark("filters_loaded", new File(HADOOP_GREP).getName());

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);

    b.mark("disk_loaded");

    EventUtil.dumpAll(mem, snk);
    b.mark(snk.getName() + " done");

    Histogram<String> histo = snk.getHistogram();
    System.out.println(histo);
    
    // from grep | wc
    Assert.assertEquals(230659, histo.get("NullPointerException"));
    Assert.assertEquals(2916, histo.get("ConnectException"));
    Assert.assertEquals(230663, histo.get("Lost tracker"));
    Assert.assertEquals(166834, histo.get("mapred.TaskTracker: Resending"));
    
    
    b.done();
  }
}
