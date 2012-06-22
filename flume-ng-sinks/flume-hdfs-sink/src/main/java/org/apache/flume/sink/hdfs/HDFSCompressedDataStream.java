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
import org.apache.flume.Context;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSCompressedDataStream implements HDFSWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(HDFSCompressedDataStream.class);

  private FSDataOutputStream fsOut;
  private CompressionOutputStream cmpOut;
  private boolean isFinished = false;

  @Override
  public void configure(Context context) {
    // no-op
  }

  @Override
  public void open(String filePath, FlumeFormatter fmt) throws IOException {
    DefaultCodec defCodec = new DefaultCodec();
    CompressionType cType = CompressionType.BLOCK;
    open(filePath, defCodec, cType, fmt);
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
      CompressionType cType, FlumeFormatter fmt) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile
    (dstPath)) {
      fsOut = hdfs.append(dstPath);
    } else {
      fsOut = hdfs.create(dstPath);
    }
    cmpOut = codec.createOutputStream(fsOut);
    isFinished = false;
  }

  @Override
  public void append(Event e, FlumeFormatter fmt) throws IOException {
    if (isFinished) {
      cmpOut.resetState();
      isFinished = false;
    }
    byte[] bValue = fmt.getBytes(e);
    cmpOut.write(bValue);
  }

  @Override
  public void sync() throws IOException {
    // We must use finish() and resetState() here -- flush() is apparently not
    // supported by the compressed output streams (it's a no-op).
    // Also, since resetState() writes headers, avoid calling it without an
    // additional write/append operation.
    // Note: There are bugs in Hadoop & JDK w/ pure-java gzip; see HADOOP-8522.
    if (!isFinished) {
      cmpOut.finish();
      isFinished = true;
    }
    fsOut.flush();
    fsOut.sync();
  }

  @Override
  public void close() throws IOException {
    sync();
    cmpOut.close();
  }

}
