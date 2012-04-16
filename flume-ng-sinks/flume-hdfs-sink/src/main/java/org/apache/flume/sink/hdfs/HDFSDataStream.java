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
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

public class HDFSDataStream implements HDFSWriter {
  private FSDataOutputStream outStream;

  private String serializerType;
  private Context serializerContext;
  private EventSerializer serializer;

  @Override
  public void configure(Context context) {
    serializerType = context.getString("serializer", "TEXT");
    serializerContext =
        new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
  }

  @Override
  public void open(String filePath, FlumeFormatter fmt) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    boolean appending = false;
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile
            (dstPath)) {
      outStream = hdfs.append(dstPath);
      appending = true;
    } else {
      outStream = hdfs.create(dstPath);
    }

    serializer = EventSerializerFactory.getInstance(
        serializerType, serializerContext, outStream);
    if (appending && !serializer.supportsReopen()) {
      outStream.close();
      serializer = null;
      throw new IOException("serializer (" + serializerType +
          ") does not support append");
    }

    if (appending) {
      serializer.afterReopen();
    } else {
      serializer.afterCreate();
    }
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
      CompressionType cType, FlumeFormatter fmt) throws IOException {
    open(filePath, fmt);
  }

  @Override
  public void append(Event e, FlumeFormatter fmt) throws IOException {
    // shun flumeformatter...
    serializer.write(e);
  }

  @Override
  public void sync() throws IOException {
    serializer.flush();
    outStream.flush();
    outStream.sync();
  }

  @Override
  public void close() throws IOException {
    serializer.flush();
    serializer.beforeClose();
    outStream.flush();
    outStream.sync();
    outStream.close();
  }

}
