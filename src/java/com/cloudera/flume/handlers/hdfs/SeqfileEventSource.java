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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.google.common.base.Preconditions;

/**
 * This is an iterator for a sequence file.
 */
public class SeqfileEventSource extends EventSource.Base {
  final static Logger LOG =
      Logger.getLogger(SeqfileEventSource.class.getName());

  private String fname;
  private SequenceFile.Reader reader;

  public SeqfileEventSource(String fname) {
    this.fname = fname;
  }

  /**
   * We assume that the file is local.
   */
  static public SeqfileEventSource openLocal(String fname) throws IOException {
    SeqfileEventSource src = new SeqfileEventSource(fname);
    src.open();
    return src;
  }

  public Event next() throws IOException {
    Preconditions.checkNotNull(reader);
    WriteableEventKey k = new WriteableEventKey();
    WriteableEvent e = new WriteableEvent();

    boolean ok = reader.next(k, e);

    if (!ok)
      return null;

    Event evt = e.getEvent();
    updateEventProcessingStats(evt);
    return evt;
  }

  @Override
  public void close() throws IOException {
    LOG.debug("closing SeqfileEventSource " + fname);
    if (reader == null) {
      return;
    }
    reader.close();
    reader = null;
  }

  @Override
  public void open() throws IOException {
    LOG.debug("opening SeqfileEventSource " + fname);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    reader = new SequenceFile.Reader(fs, new Path(fname), conf);
  }

  @Override
  public String toString() {
    return "SeqfileEventSource name=" + fname;
  }

  public static void main(String[] argv) throws IOException {
    if (argv.length < 1) {
      System.err.println("need to specify source file");
      System.exit(-1);
    }

    SeqfileEventSource src = SeqfileEventSource.openLocal(argv[0]);
    do {
      Event e = src.next();
      if (e == null)
        break;

      System.out.println(e);
    } while (true);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        if (argv.length != 1) {
          throw new IllegalArgumentException("usage: seqfile(filename)");
        }

        return new SeqfileEventSource(argv[0]);
      }

    };
  }

}
