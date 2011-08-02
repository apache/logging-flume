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
package com.cloudera.util.dirwatcher;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This class watches a specified directory for deletions, creations and
 * "age off" events. It spawns a thread that periodically checks the directory.
 */
public class DirWatcher {
  static Logger LOG = Logger.getLogger(DirWatcher.class);

  final private List<DirChangeHandler> list = Collections
      .synchronizedList(new ArrayList<DirChangeHandler>());
  private File dir;
  private volatile boolean done = false;
  private Set<File> previous = new HashSet<File>();
  private long sleep_ms;
  private Periodic thread;
  private FileFilter filter;

  /**
   * checkperiod is the amount of time in milliseconds between directory polls.
   */
  public DirWatcher(File dir, FileFilter filter, long checkPeriod) {
    Preconditions.checkNotNull(dir);
    Preconditions.checkArgument(dir.isDirectory(), dir + " is not a directory");

    this.thread = null;
    this.dir = dir;
    this.sleep_ms = checkPeriod;
    this.filter = filter;
  }

  /**
   * Start the directory watching. This implementation uses a thread to poll
   * periodically. If called multiple times, it will only start a single thread.
   * This is not threadsafe
   */
  public void start() {
    if (thread != null) {
      LOG.warn("Dir watcher already started!");
      return;
    }
    this.thread = new Periodic();
    this.thread.start();
    LOG.info("Started dir watcher thread");
  }

  /**
   * This attempts to stop the dir watching. With this thread-based
   * implementation it, blocks until the thread is done. This is not thread
   * safe.
   */
  public void stop() {
    if (thread == null) {
      LOG.warn("DirWatcher already stopped");
      return;
    }

    done = true;

    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    } // waiting for thread to complete.
    LOG.info("Stopped dir watcher thread");
    thread = null;
  }

  /**
   * This thread periodically checks a directory for updates
   */
  class Periodic extends Thread {
    Periodic() {
      super("DirWatcher");
    }

    public void run() {
      try {
        while (!done) {
          try {
            check();
            Clock.sleep(sleep_ms);
          } catch (NumberFormatException nfe) {
            LOG.warn("wtf ", nfe);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * This the core check method that updates information from the previous poll
   * and fires events based on changes.
   */
  public void check() {
    File[] files = dir.listFiles();
    if (files == null) {
      LOG.warn("dir " + dir.getAbsolutePath() + " does not exist?!");
      return;
    }
    Set<File> newfiles = new HashSet<File>(Arrays.asList(files));

    // figure out what was created
    Set<File> addedFiles = new HashSet<File>(newfiles);
    addedFiles.removeAll(previous);
    for (File f : addedFiles) {
      if (filter.isSelected(f)) {
        fireCreatedFile(f);
      }
    }

    // figure out what was deleted
    Set<File> removedFiles = new HashSet<File>(previous);
    removedFiles.removeAll(newfiles);
    for (File f : removedFiles) {
      if (filter.isSelected(f)) {
        fireDeletedFile(f);
      }
    }

    previous = newfiles;
  }

  /**
   * Add a handler callback object.
   */
  public void addHandler(DirChangeHandler dch) {
    list.add(dch);
  }

  /**
   * Fire the 'created' callback on all handlers.
   */
  public void fireCreatedFile(File f) {

    // make copy so it is thread safe
    DirChangeHandler[] hs = list.toArray(new DirChangeHandler[0]);
    for (DirChangeHandler h : hs) {
      h.fileCreated(f);
    }

  }

  /**
   * Fire the 'deleted' callback on all handlers.
   */
  public void fireDeletedFile(File f) {
    // make copy so it is thread safe
    DirChangeHandler[] hs = list.toArray(new DirChangeHandler[0]);
    for (DirChangeHandler h : hs) {
      h.fileDeleted(f);
    }
  }

}
