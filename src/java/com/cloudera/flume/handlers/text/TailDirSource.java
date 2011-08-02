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
package com.cloudera.flume.handlers.text;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.TailSource.Cursor;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.dirwatcher.DirChangeHandler;
import com.cloudera.util.dirwatcher.DirWatcher;
import com.cloudera.util.dirwatcher.RegexFileFilter;
import com.google.common.base.Preconditions;

/**
 * This source tails all the file in a directory that match a specified regular
 * expression.
 */
public class TailDirSource extends EventSource.Base {
  public static final Logger LOG = LoggerFactory.getLogger(TailDirSource.class);
  private DirWatcher watcher;
  private TailSource tail;
  final private File dir;
  final private String regex;
  private final boolean startFromEnd;
  // Indicates whether dir was checked. It is false before source is open
  // and set to true after the first check of a dir
  private volatile boolean dirChecked = false;

  final private AtomicLong filesAdded = new AtomicLong();
  final private AtomicLong filesDeleted = new AtomicLong();

  final public static String A_FILESADDED = "filesAdded";
  final public static String A_FILESDELETED = "filesDeleted";
  final public static String A_FILESPRESENT = "filesPresent";

  public TailDirSource(File f, String regex) {
    this(f, regex, false);
  }

  public TailDirSource(File f, String regex, boolean startFromEnd) {
    Preconditions.checkArgument(f != null, "File should not be null!");
    Preconditions.checkArgument(regex != null,
        "Regex filter should not be null");

    this.dir = f;
    this.regex = regex;
    this.startFromEnd = startFromEnd;

    // 100 ms between checks
    this.tail = new TailSource(100);
  }

  /**
   * Must be synchronized to isolate watcher
   */
  @Override
  synchronized public void open() throws IOException {
    Preconditions.checkState(watcher == null,
        "Attempting to open an already open TailDirSource (" + dir + ", \""
            + regex + "\")");
    // 250 ms between checks
    this.watcher = new DirWatcher(dir, new RegexFileFilter(regex), 250);
    synchronized (watcher) {
      this.watcher.addHandler(new DirChangeHandler() {
        Map<String, TailSource.Cursor> curmap = new HashMap<String, TailSource.Cursor>();

        @Override
        public void fileCreated(File f) {
          // Add a new file to the multi tail.
          if (f.isDirectory()) {
            LOG.debug("Tail dir will not read or recurse "
                + "into subdirectory " + f);
            return;
          }

          LOG.info("added file " + f);
          Cursor c;
          if (startFromEnd && !dirChecked) {
            // init cursor positions on first dir check when startFromEnd is set to true
            c = new Cursor(tail.sync, f, f.length(), f.length(), f.lastModified());
            try {
              c.initCursorPos();
            } catch (InterruptedException e) {
              LOG.error("Initializing of cursor failed", e);
              c.close();
              return;
            }
          } else {
            c = new Cursor(tail.sync, f);
          }

          curmap.put(f.getName(), c);
          tail.addCursor(c);
          filesAdded.incrementAndGet();
        }

        @Override
        public void fileDeleted(File f) {
          LOG.info("removed file " + f);
          Cursor c = curmap.remove(f.getName());
          tail.removeCursor(c);
          filesDeleted.incrementAndGet();
        }

      });

      // Separate check is needed to init cursor positions
      // (to the end of the files in dir)
      if (startFromEnd) {
        this.watcher.check();
      }
      dirChecked = true;
      this.watcher.start();
    }
    tail.open();
  }

  @Override
  synchronized public void close() throws IOException {
    tail.close();
    synchronized (watcher) {
      this.watcher.stop();
      this.watcher = null;
    }
  }

  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_FILESADDED, filesAdded.get());
    rpt.setLongMetric(A_FILESDELETED, filesDeleted.get());
    rpt.setLongMetric(A_FILESPRESENT, tail.cursors.size());
    return rpt;
  }

  @Override
  public Event next() throws IOException {
    // this cannot be in synchronized because it has a
    // blocking call to a queue inside it.
    Event e = tail.next();

    synchronized (this) {
      updateEventProcessingStats(e);
      return e;
    }
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length >= 1 && argv.length <= 3,
                "usage: tailDir(\"dirname\"[, fileregex=\".*\"[, startFromEnd=false]])");

        String regex = ".*"; // default to accepting all
        if (argv.length >= 2) {
          regex = argv[1];
        }
        boolean startFromEnd = false;
        if (argv.length >= 3) {
          startFromEnd = Boolean.parseBoolean(argv[2]);
        }
        return new TailDirSource(new File(argv[0]), regex, startFromEnd);
      }
    };
  }
}
