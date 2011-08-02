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

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.hdfs.WriteableEventKey;
import com.cloudera.util.Benchmark;

public class PerfHdfsIO extends TestCase implements ExamplePerfData {

  public void testCopy() throws IOException {

    Benchmark b = new Benchmark("hdfs seqfile copy");
    b.mark("begin");

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    b.mark("disk_loaded");

    File tmp = File.createTempFile("test", "tmp");
    tmp.deleteOnExit();
    SeqfileEventSink sink = new SeqfileEventSink(tmp);
    sink.open();
    b.mark("localdisk_write_started");

    EventUtil.dumpAll(mem, sink);

    b.mark("local_disk_write done");

    sink.close();

    FlumeConfiguration conf = FlumeConfiguration.get();
    Path src = new Path(tmp.getAbsolutePath());
    Path dst = new Path("hdfs://localhost/testfile");
    FileSystem hdfs = dst.getFileSystem(conf);
    hdfs.deleteOnExit(dst);

    b.mark("hdfs_copy_started");
    hdfs.copyFromLocalFile(src, dst);
    b.mark("hdfs_copy_done");
    hdfs.close();
    b.done();
  }

  public void testDirectWrite() throws IOException {

    Benchmark b = new Benchmark("hdfs seqfile write");
    b.mark("begin");

    TextFileSource txt = new TextFileSource(HADOOP_DATA[0]);
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();
    b.mark("disk_loaded");

    FlumeConfiguration conf = FlumeConfiguration.get();
    Path path = new Path("hdfs://localhost/testfile");
    FileSystem hdfs = path.getFileSystem(conf);
    hdfs.deleteOnExit(path);

    Writer w = SequenceFile.createWriter(hdfs, conf, path,
        WriteableEventKey.class, WriteableEvent.class);
    b.mark("hdfs_fileopen_started");

    Event e = null;
    while ((e = mem.next()) != null) {
      // writing
      w.append(new WriteableEventKey(e), new WriteableEvent(e));
    }
    w.close();
    b.mark("seqfile_hdfs_write");

    hdfs.close();
    b.done();
  }

}
