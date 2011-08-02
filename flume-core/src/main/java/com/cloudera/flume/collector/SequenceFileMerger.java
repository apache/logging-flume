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
package com.cloudera.flume.collector;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.hdfs.WriteableEventKey;

public class SequenceFileMerger {

  public static void main(String[] argv) throws IOException {
    if (argv.length < 2) {
      System.out.println("need to specify target output file, and source dir");
      System.exit(-1);
    }

    FlumeConfiguration conf = FlumeConfiguration.get();
    Path dst = new Path(argv[0]);
    String src = argv[1];
    FileSystem fs = FileSystem.getLocal(conf);

    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst,
        WriteableEventKey.class, WriteableEvent.class);

    long count = 0;
    long fcount = 0;
    File[] files = new File(src).listFiles();
    for (File f : files) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(f
          .getAbsolutePath()), conf);
      System.out.println(f.getName());
      fcount++;
      boolean hasNext = true;
      while (hasNext) {
        WriteableEventKey k = new WriteableEventKey();
        WriteableEvent e = new WriteableEvent();
        hasNext = reader.next(k, e);
        if (hasNext) {
          writer.append(k, e);
          count++;
        }
      }
      reader.close();
    }

    writer.close();
    System.out
        .println("Wrote " + count + " entries from " + fcount + " files.");
  }
}
