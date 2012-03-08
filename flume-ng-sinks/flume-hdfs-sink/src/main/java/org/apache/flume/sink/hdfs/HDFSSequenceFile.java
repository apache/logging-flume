/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

public class HDFSSequenceFile implements HDFSWriter {

  private SequenceFile.Writer writer;

  public HDFSSequenceFile() {
    writer = null;
  }

  @Override
  public void open(String filePath, FlumeFormatter fmt) throws IOException {
    open(filePath, null, CompressionType.NONE, fmt);
  }

  @Override
  public void open(String filePath, CompressionCodec codeC,
      CompressionType compType, FlumeFormatter fmt) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    if (conf.getBoolean("hdfs.append.support", false) == true) {
      FSDataOutputStream outStream = hdfs.append(dstPath);
      writer = SequenceFile.createWriter(conf, outStream, fmt.getKeyClass(),
          fmt.getValueClass(), compType, codeC);
    } else {
      writer = SequenceFile.createWriter(hdfs, conf, dstPath,
          fmt.getKeyClass(), fmt.getValueClass(), compType, codeC);
    }
  }

  @Override
  public void append(Event e, FlumeFormatter formatter) throws IOException {
    writer.append(formatter.getKey(e), formatter.getValue(e));
  }

  @Override
  public void sync() throws IOException {
    writer.syncFs();
  }

  @Override
  public void close() throws IOException {
    writer.close();
    writer = null;
  }
}
