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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSequenceFile implements HDFSWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(HDFSSequenceFile.class);
  private SequenceFile.Writer writer;
  private String writeFormat;
  private Context serializerContext;
  private SeqFileFormatter formatter;
  private boolean useRawLocalFileSystem;

  public HDFSSequenceFile() {
    writer = null;
  }

  @Override
  public void configure(Context context) {
    // use binary writable format by default
    writeFormat = context.getString("hdfs.writeFormat", SeqFileFormatterType.Writable.name());
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    serializerContext = new Context(
            context.getSubProperties(SeqFileFormatterFactory.CTX_PREFIX));
    formatter = SeqFileFormatterFactory
            .getFormatter(writeFormat, serializerContext);
    logger.info("writeFormat = " + writeFormat + ", UseRawLocalFileSystem = "
        + useRawLocalFileSystem);
  }

  @Override
  public void open(String filePath) throws IOException {
    open(filePath, null, CompressionType.NONE);
  }

  @Override
  public void open(String filePath, CompressionCodec codeC,
      CompressionType compType) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);
    if(useRawLocalFileSystem) {
      if(hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem)hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system " +
            "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile
            (dstPath)) {
      FSDataOutputStream outStream = hdfs.append(dstPath);
      writer = SequenceFile.createWriter(conf, outStream, formatter.getKeyClass(),
          formatter.getValueClass(), compType, codeC);
    } else {
      writer = SequenceFile.createWriter(hdfs, conf, dstPath,
          formatter.getKeyClass(), formatter.getValueClass(), compType, codeC);
    }
  }

  @Override
  public void append(Event e) throws IOException {
    for (SeqFileFormatter.Record record : formatter.format(e)) {
      writer.append(record.getKey(), record.getValue());
    }
  }

  @Override
  public void sync() throws IOException {
    writer.syncFs();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
