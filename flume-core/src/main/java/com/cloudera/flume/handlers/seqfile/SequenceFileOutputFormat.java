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

package com.cloudera.flume.handlers.seqfile;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.hdfs.WriteableEventKey;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.flume.handlers.text.output.AbstractOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import static org.apache.hadoop.mapred.SequenceFileOutputFormat.getOutputCompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputFormat} for writing Flume events to Hadoop Sequence Files.
 * 
 * This class is not thread safe.
 */
public class SequenceFileOutputFormat extends AbstractOutputFormat {
  private static final Logger LOG =
      LoggerFactory.getLogger(SequenceFileOutputFormat.class);

  private static final String NAME = "seqfile";
  
  private CompressionType compressionType;
  private CompressionCodec codec;
  
  private OutputStream cachedOut;
  private Writer writer;
  
  public SequenceFileOutputFormat() {
    this(getOutputCompressionType(new JobConf(FlumeConfiguration.get())),
         new DefaultCodec());
  }
  
  public SequenceFileOutputFormat(CompressionType compressionType,
      CompressionCodec codec) {
    this.compressionType = compressionType;
    this.codec = codec;
  }

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    if (writer == null) {
      cachedOut = o;
      FSDataOutputStream fsOut;
      if (o instanceof FSDataOutputStream) {
        fsOut = (FSDataOutputStream) o; 
      } else {
        fsOut = new FSDataOutputStream(o, null);
      }
      writer = SequenceFile.createWriter(FlumeConfiguration.get(), fsOut,
              WriteableEventKey.class, WriteableEvent.class,
              compressionType, codec);
    }
    if (cachedOut != o) {
      // different output than last time, fail here
      throw new IOException(
          "OutputFormat instance can only write to the same OutputStream");
    }
    writer.append(new WriteableEventKey(e), new WriteableEvent(e));
  }
  

  
  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {

      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length <= 1, "usage: seqfile([codec])");

        OutputFormat format;
        
        String codecName = null;
        if (args.length > 0) {
          codecName = args[0];
          FlumeConfiguration conf = FlumeConfiguration.get();
          CompressionType compressionType = getOutputCompressionType(new JobConf(conf));
          CompressionCodec codec = CustomDfsSink.getCodec(conf, codecName);
          format = new SequenceFileOutputFormat(compressionType, codec);
        } else {
          format = new SequenceFileOutputFormat();
        }

        format.setBuilder(this);

        return format;
      }

      @Override
      public String getName() {
        return NAME;
      }

    };
  }

}
