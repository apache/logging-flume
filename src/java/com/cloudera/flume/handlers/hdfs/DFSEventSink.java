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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;

/**
 * Writes events the a file give a hadoop uri path. If no uri is specified It
 * defaults to the set by the given configured by fs.default.name config
 * variable
 */
public class DFSEventSink extends EventSink.Base {
  final static Logger LOG = Logger.getLogger(DFSEventSink.class.getName());
  String path;
  Writer writer = null;
  // We keep a - potentially unbounded - set of writers around to deal with
  // different tags on events. Therefore this feature should be used with some
  // care (where the set of possible paths is small) until we do something
  // more sensible with resource management.
  final Map<String, Writer> sfWriters = new HashMap<String, Writer>();

  // Used to short-circuit around doing regex matches when we know there are
  // no templates to be replaced.
  boolean shouldSub = false;

  public DFSEventSink(String path) {
    this.path = path;
    shouldSub = Event.containsTag(path);
  }

  protected Writer openWriter(String p) throws IOException {
    LOG.info("Opening " + p);
    FlumeConfiguration conf = FlumeConfiguration.get();

    Path dstPath = new Path(p);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    Writer w =
        SequenceFile.createWriter(hdfs, conf, dstPath, WriteableEventKey.class,
            WriteableEvent.class);

    return w;
  }

  /**
   * Writes the message to an HDFS file whose path is substituted with tags
   * drawn from the supplied event
   */
  @Override
  public void append(Event e) throws IOException {
    Writer w = writer;
    if (shouldSub) {
      String realPath = e.escapeString(path);
      w = sfWriters.get(realPath);
      if (w == null) {
        w = openWriter(realPath);
        sfWriters.put(realPath, w);
      }
    }
    w.append(new WriteableEventKey(e), new WriteableEvent(e));
  }

  @Override
  public void close() throws IOException {
    if (shouldSub) {
      for (Entry<String, Writer> e : sfWriters.entrySet()) {
        LOG.info("Closing " + e.getKey());
        e.getValue().close();
      }
    } else {
      LOG.info("Closing " + path);
      writer.close();
      writer = null;
    }
  }

  @Override
  public void open() throws IOException {
    if (!shouldSub) {
      writer = openWriter(path);
    }
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length != 1) {
          // TODO (jon) make this message easier.
          throw new IllegalArgumentException(
              "usage: dfs(\"[(hdfs|file|s3n|...)://namenode[:port]]/path\")");
        }
        return new DFSEventSink(args[0]);
      }
    };
  }
}
