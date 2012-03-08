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
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;

public class HDFSCompressedDataStream implements HDFSWriter {

  private CompressionOutputStream outStream;

  @Override
  public void open(String filePath, FlumeFormatter fmt) throws IOException {
    DefaultCodec defCodec = new DefaultCodec();
    CompressionType cType = CompressionType.BLOCK;
    open(filePath, defCodec, cType, fmt);
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
      CompressionType cType, FlumeFormatter fmt) throws IOException {

    FSDataOutputStream fsOutStream;
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    if (conf.getBoolean("hdfs.append.support", false) == true) {
      fsOutStream = hdfs.append(dstPath);
    } else {
      fsOutStream = hdfs.create(dstPath);
    }
    outStream = codec.createOutputStream(fsOutStream);
  }

  @Override
  public void append(Event e, FlumeFormatter fmt) throws IOException {
    byte[] bValue = fmt.getBytes(e);
    outStream.write(bValue, 0, bValue.length);
  }

  @Override
  public void sync() throws IOException {
    outStream.finish();
  }

  @Override
  public void close() throws IOException {
    outStream.close();
  }

}
