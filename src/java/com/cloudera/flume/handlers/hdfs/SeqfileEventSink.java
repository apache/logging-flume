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
package com.cloudera.flume.handlers.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FlushingSequenceFileWriter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.FileUtil;
import com.google.common.base.Preconditions;

/**
 * This is an event sink that dumps to a hadoop sequence file on the local file
 * system..
 */
public class SeqfileEventSink extends EventSink.Base {
  final static Logger LOG = Logger.getLogger(SeqfileEventSink.class);

  private SequenceFile.Writer writer;
  private long count = 0;
  private boolean bufferedIO = false;
  private String tag;
  private File f;

  public SeqfileEventSink(File f) throws IOException {
    this.f = f;
    this.tag = f.getName();
    LOG.info("constructed new seqfile event sink: file=" + f);
  }

  /*
   * This is assumed to be open
   */
  public void open() throws IOException {
    LOG.debug("opening " + f);

    // need absolute file to get full path. (pwd files have not parent file)
    // make directories if necessary
    if (!FileUtil.makeDirs(f.getAbsoluteFile().getParentFile())) {
      throw new IOException("Unable to create directory"
          + f.getAbsoluteFile().getParentFile() + " for writing");
    }

    Configuration conf = FlumeConfiguration.get();
    try {
      writer =
          FlushingSequenceFileWriter.createWriter(conf, f,
              WriteableEventKey.class, WriteableEvent.class);
    } catch (FileNotFoundException fnfe) {
      LOG.error("Possible permissions problem when creating " + f, fnfe);
      throw fnfe;
    }
  }

  /**
   * @throws IOException
   * 
   */
  public void close() throws IOException {
    LOG.debug("closing");
    if (writer == null) {
      // allow closing twice.
      return;
    }
    writer.close();
    writer = null;
    LOG.info("closed");
  }

  public void append(Event e) throws IOException {
    Preconditions.checkNotNull(writer,
        "Attempt to append to a sink that is closed!");

    WriteableEvent we = new WriteableEvent(e);
    writer.append(we.getEventKey(), we);

    // flush if we are not buffering
    if (!bufferedIO)
      writer.sync(); // this isn't flushing or sync'ing on local file system :(

    count++;
  }

  public void setBufferIO(boolean bufferedIO) {
    this.bufferedIO = bufferedIO;
  }

  public String getTag() {
    return tag;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        Preconditions.checkArgument(args.length == 1,
            "usage: seqfile(filename)");
        try {
          return new SeqfileEventSink(new File(args[0]));
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to open file for writing "
              + args[0]);
        }
      }
    };
  }

}
