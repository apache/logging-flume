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

import org.junit.Test;

import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSource;
import com.cloudera.util.Benchmark;

/**
 * This performance test tests the throughput of various disk reading and
 * writing sources and sinks.
 */
public class PerfDiskIO {

  @Test
  public void testWrite() throws IOException, InterruptedException {
    Benchmark b = new Benchmark("seqfile write");
    b.mark("begin");
    MemorySinkSource mem = FlumeBenchmarkHarness.synthInMem();
    b.mark("disk_loaded");

    File tmp = File.createTempFile("test", "tmp");
    tmp.deleteOnExit();
    SeqfileEventSink sink = new SeqfileEventSink(tmp);
    sink.open();
    b.mark("receiver_started");

    EventUtil.dumpAll(mem, sink);

    b.mark("seqfile_disk_write");

    sink.close();
    b.mark("seqfile size", tmp.length());
    b.done();
    mem = null; // allow mem to be freed.

    // //////// second phase using the file written in previous phase.
    Benchmark b2 = new Benchmark("seqfile_disk_read");
    b2.mark("begin");

    SeqfileEventSource seq = new SeqfileEventSource(tmp.getAbsolutePath());
    seq.open();
    MemorySinkSource mem2 = new MemorySinkSource();
    EventUtil.dumpAll(seq, mem2);
    seq.close();
    b2.mark("seqfile_loaded");

    b2.done();
  }

}
