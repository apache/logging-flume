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
package com.cloudera.flume.handlers.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;
import com.google.common.base.Preconditions;

/**
 * Writes events the a file give a hadoop uri path. If no uri is specified It
 * defaults to the set by the given configured by fs.default.name config
 * variable. The user can specify an output format for the file. If none is
 * specified the default set by flume.collector.outputformat in the flume
 * configuration file is used.
 * 
 * 
 * 
 * TODO (jon) refactor this to be sane. Not happening now.
 */
public class HiveNotifyingDfsSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(HiveNotifyingDfsSink.class);
  final String dirpath;
  final OutputFormat format;
  final String hivetable;
  final HiveDirCreatedHandler handler;

  EventSink writer = null;

  // We keep a - potentially unbounded - set of writers around to deal with
  // different tags on events. Therefore this feature should be used with some
  // care (where the set of possible paths is small) until we do something
  // more sensible with resource management.
  final Map<String, EventSink> sfWriters = new HashMap<String, EventSink>();

  // Used to short-circuit around doing regex matches when we know there are
  // no templates to be replaced.
  private String filename = "";
  protected String absolutePath = "";

  public HiveNotifyingDfsSink(String path, String filename, String hivetable,
      OutputFormat o, HiveDirCreatedHandler handler) {
    this.dirpath = path;
    this.filename = filename;
    this.format = o;
    absolutePath = path;
    this.hivetable = hivetable;
    if (filename != null && filename.length() > 0) {
      if (!absolutePath.endsWith(Path.SEPARATOR)) {
        absolutePath += Path.SEPARATOR;
      }
      absolutePath += this.filename;
    }

    if (!(o instanceof AvroJsonOutputFormat)) {
      LOG
          .warn("Currently, hive only supports only AvroJson output format SerDe.");
    }

    this.handler = handler;
  }

  public HiveNotifyingDfsSink(String path, String filename, String hivetable) {
    this(path, filename, hivetable, getDefaultOutputFormat(),
        new DefaultHandler());
  }

  static class DefaultHandler implements HiveDirCreatedHandler {
    @Override
    public void handleNotification(HiveDirCreatedNotification notif) {
      LOG.info("Notifying Hive Metastore with ready event " + notif);
      // TODO (carl) This should be queued to a scheduler that will spawn
      // a thread to handle the notification.
    }
  };

  static class DedupDefaultHandler implements HiveDirCreatedHandler {
    HashSet<String> cache = new HashSet<String>();
    HiveDirCreatedHandler simple;

    public DedupDefaultHandler(HiveDirCreatedHandler hfrh) {
      this.simple = hfrh;
    }

    public void handleNotification(HiveDirCreatedNotification notif) {
      if (cache.contains(notif.getNotifDir())) {
        return;
      }

      simple.handleNotification(notif);
      cache.add(notif.getNotifDir());
    }
  }

  static protected OutputFormat getDefaultOutputFormat() {
    try {
      return FormatFactory.get().getOutputFormat(
          FlumeConfiguration.get().getDefaultOutputFormat());
    } catch (FlumeSpecException e) {
      LOG.warn("format from conf file not found, using default", e);
      return new RawOutputFormat();
    }
  }

  protected EventSink getWriter(Event e) throws IOException,
      InterruptedException {
    final Event evt = e;
    String realpath = e.escapeString(absolutePath);
    EventSink w = sfWriters.get(realpath);
    if (w != null) {
      // uses already existing sink.
      return w;
    }

    // sink does exist for event, create it.
    LOG.info("Opening " + realpath);

    w = new CustomDfsSink(realpath, format);
    SinkCloseNotifier<EventSink, HiveDirCreatedNotification> notif = new SinkCloseNotifier<EventSink, HiveDirCreatedNotification>(
        w) {
      @Override
      public HiveDirCreatedNotification getNotificationEvent() {
        // take the dir part of the path
        String escdirpath = evt.escapeString(dirpath);
        Map<String, String> partitions = evt.getEscapeMapping(dirpath);
        return new HiveDirCreatedNotification(hivetable, escdirpath, partitions);
      }

      @Override
      public void notify(HiveDirCreatedNotification e) {
        handler.handleNotification(e);
      }
    };
    notif.open();
    sfWriters.put(realpath, notif);
    return notif;
  }

  /**
   * Writes the message to an HDFS file whose path is substituted with tags
   * drawn from the supplied event
   */
  @Override
  public void append(Event e) throws IOException, InterruptedException {
    EventSink w = getWriter(e);
    w.append(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    for (Entry<String, EventSink> e : sfWriters.entrySet()) {
      LOG.info("Closing " + e.getKey());
      e.getValue().close();
    }
  }

  @Override
  public void open() throws IOException {
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        Preconditions.checkArgument(args.length >= 2 && args.length <= 3,
            "usage: hivedfs(\"[(hdfs|file|s3n|...)://namenode[:port]]/path\""
                + ", \"hivetable\", [, outputformat ])");

        String format = FlumeConfiguration.get().getDefaultOutputFormat();
        String hivetable = args[1];
        if (args.length >= 3) {
          format = args[2];
        }

        OutputFormat o;
        try {
          o = FormatFactory.get().getOutputFormat(format);
        } catch (FlumeSpecException e) {
          LOG.warn("Illegal format type " + format + ".", e);
          o = null;
        }
        Preconditions.checkArgument(o != null, "Illegal format type " + format
            + ".");

        return new HiveNotifyingDfsSink(args[0], "", hivetable, o,
            new DefaultHandler());
      }
    };
  }
}
