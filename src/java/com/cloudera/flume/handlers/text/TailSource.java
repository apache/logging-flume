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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This "tail"s a filename. Like a unix tail utility, it will wait for more
 * information to come to the file and periodically dump data as it is written.
 * It assumes that each line is a separate event.
 * 
 * This is for legacy log files where the file system is the only mechanism
 * flume has to get events. Currently, examples include apachelogs and syslog.
 * It assumes that there is one entry per line (per \n) and that these lines are
 * written atomically from the view point of this code.
 * 
 * It also has logic to deal with file rotations -- if a file is renamed and
 * then a new file is created, it will shift over to the new file. The current
 * file is read until the file pointer reaches the end of the file. It will wait
 * there until periodic checks notice that the file has become longer. If the
 * file "shrinks" we assume that the file has been replaced with a new log file.
 * 
 * TODO (jon) This is not perfect.
 * 
 * This assumes ASCII encoded files -- if \n's appear in a UTF8 encoding entries
 * will not be correct.
 * 
 * There is a possibility for inconsistent conditions when are logs rotated.
 * 
 * 1) If rotation periods are faster than periodic checks, a file may be missed.
 * (this mimics gnu-tail semantics here)
 * 
 * 2) Truncations of files will reset the file pointer. This is because the Java
 * file api does not a mechanism to get the inode of a particular file, so there
 * is no way to differentiate between a new file or a truncated file!
 * 
 * 3) If a file is being read, is moved, and replaced with another file of
 * exactly the same size in a particular window, the data in the new file may be
 * lost. If the original file has been completely read and then replaced with a
 * file of the same length this problem will not occur. (See
 * TestTailSource.readRotatePrexistingFailure vs
 * TestTailSource.readRotatePrexistingSameSizeWithNewModetime)
 * 
 * Ideally this would use the inode number of file handle number but didn't find
 * java api to get these, or Java 7's WatchSevice file watcher API.
 */
public class TailSource extends EventSource.Base {
  private final static Logger LOG = Logger
      .getLogger(TailSource.class.getName());

  private static int thd_count = 0;
  private volatile boolean done = false;

  private final long sleepTime; // millis
  final List<Cursor> cursors = new ArrayList<Cursor>();
  private final List<Cursor> newCursors = new ArrayList<Cursor>();
  private final List<Cursor> rmCursors = new ArrayList<Cursor>();
  // We "queue" only allowing a single Event.

  final SynchronousQueue<Event> sync = new SynchronousQueue<Event>();
  private TailThread thd = null;

  /**
   * Constructor for backwards compatibility.
   */
  public TailSource(File f, long offset, long waitTime) {
    this(f, offset, waitTime, false);
  }

  /**
   * Specify the file, the starting offset (something >=0) and wait time between
   * checks in millis. If startFromEnd is set, begin reading the file at the
   * end, not the beginning.
   */
  public TailSource(File f, long offset, long waitTime, boolean startFromEnd) {
    Preconditions.checkArgument(offset >= 0 || startFromEnd,
        "offset needs to be >=0 or startFromEnd needs to be true");
    Preconditions.checkNotNull(f);
    Preconditions.checkArgument(waitTime > 0);
    this.sleepTime = waitTime;

    // add initial cursor.
    long fileLen = f.length();
    long readOffset = startFromEnd ? fileLen : offset;
    long modTime = f.lastModified();
    Cursor c = new Cursor(sync, f, readOffset, fileLen, modTime);
    cursors.add(c);
  }

  /**
   * This creates an empty tail source. It expects something else to add cursors
   * to it
   */
  public TailSource(long waitTime) {
    this.sleepTime = waitTime;
  }

  /**
   * To support multiple tail readers, we have a Cursor for each file name
   */
  static class Cursor {
    final BlockingQueue<Event> sync;
    final File file;
    RandomAccessFile raf = null;
    long lastMod;
    long lastReadOffset;
    long lastFileLen;

    Cursor(BlockingQueue<Event> sync, File f) {
      this(sync, f, 0, 0, 0);
    }

    Cursor(BlockingQueue<Event> sync, File f, long lastReadOffset,
        long lastFileLen, long lastMod) {
      this.sync = sync;
      this.file = f;
      this.lastReadOffset = lastReadOffset;
      this.lastFileLen = lastFileLen;
      this.lastMod = lastMod;
    }

    void initCursorPos() {
      try {
        raf = new RandomAccessFile(file, "r");
        raf.seek(lastReadOffset);
      } catch (FileNotFoundException e) {
        resetRAF();
      } catch (IOException e) {
        resetRAF();
      }
    }

    /**
     * Restart random accessfile cursor
     */
    void resetRAF() {
      raf = null;
      lastReadOffset = 0;
      lastMod = 0;
    }

    /**
     * There is a race here on file system states -- a truly evil dir structure
     * can change things between these calls. Nothing we can really do though.
     * 
     * Ideally we would get and compare inode numbers, but in java land, we
     * can't do that.
     * 
     * returns true if changed, false if not.
     */
    boolean checkForUpdates() throws IOException {
      LOG.debug("tail " + file + " : recheck");
      if (file.isDirectory()) {
        // exists but not a file

        IOException ioe = new IOException("Tail expects a file '" + file
            + "', but it is a dir!");
        LOG.error(ioe.getMessage());
        throw ioe;
      }

      if (!file.exists()) {
        LOG.debug("Tail '" + file + "': nothing to do, waiting for a file");
        return false; // do nothing
      }

      // oh! f exists and is a file
      try {
        if (raf != null) {
          if (lastMod == file.lastModified() && lastReadOffset == file.length()) {
            LOG.debug("Tail '" + file + "': recheck still the same");
            return false;
          }
        }

        raf = new RandomAccessFile(file, "r");
        lastMod = file.lastModified();
        lastReadOffset = raf.getFilePointer();
        lastFileLen = raf.length();
        LOG.debug("Tail '" + file + "': opened last mod=" + lastMod
            + " lastReadOffset=" + lastReadOffset + " lastFileLen="
            + lastFileLen);
        return true;
      } catch (FileNotFoundException fnfe) {
        // possible because of file system race, we can recover from this.
        LOG.debug("Tail '" + file
            + "': a file existed then disappeared, odd but continue");
        return false;
      }
    }

    /**
     * Attempt to get new data.
     * 
     * Returns true if cursor's state has changed.
     */
    boolean tailBody() throws InterruptedException {
      try {
        // no file named f currently, needs to be opened.
        if (raf == null) {
          LOG.debug("tail " + file + " : cur file is null");
          return checkForUpdates();
        }

        // get stats from raf and from f.
        long flen = file.length(); // length of filename
        long raflen = raf.length(); // length of file.
        long fmod = file.lastModified(); // ideally this has raf's last
        // modified.

        lastFileLen = raflen;

        // cases:
        if (raflen == flen && lastReadOffset == flen) {
          if (lastMod == fmod) {
            // // 3) raf len == file len, last read == file len, lastMod same ->
            // no change
            LOG.debug("tail " + file + " : no change");
            return false;
          } else {
            // // 4) raf len == file len, last read == file len, lastMod diff ?!
            // ->
            // restart file.
            LOG.debug("tail " + file
                + " : same file len, but new last mod time" + " -> reset");
            resetRAF();
            return true;
          }
        }

        // file has changed
        LOG.debug("tail " + file + " : file changed");
        LOG.debug("tail " + file + " : old size, mod time " + lastReadOffset
            + "," + lastMod);
        LOG
            .debug("tail " + file + " : new size, mod time " + flen + ","
                + fmod);

        // // 1) truncated file? -> restart file
        // file truncated?
        if (lastReadOffset > flen) {
          LOG.debug("tail " + file + " : file truncated!?");

          // normally we would check the inode, but since we cannot, we restart
          // the file.
          resetRAF();
          return true;
        }

        // I make this a rendezvous because this source is being pulled
        // copy data from current file pointer to EOF to dest.
        int len = 0;
        String str;
        while ((str = raf.readLine()) != null) {
          byte[] data = str.getBytes();

          Event e = new EventImpl(data);
          sync.put(e);
          len += data.length;

          lastReadOffset = raf.getFilePointer();
          lastMod = fmod;
        }

        if (len == 0) {
          // didn't read anyhting? raflen != filelen? restart file.
          LOG.debug("tail " + file
              + " : no progress but raflen != filelen, resetting");
          resetRAF();
          return true;
        }

        LOG.debug("tail " + file + ": read " + len + " bytes");
      } catch (IOException e) {
        LOG.debug(e.getMessage(), e);
        raf = null;
      }
      return true;
    }
  };

  /**
   * This is the main driver thread that runs through the file cursor list
   * checking for updates and sleeping if there are none.
   */
  class TailThread extends Thread {

    TailThread() {
      super("TailThread-" + thd_count++);
    }

    @Override
    public void run() {
      try {
        // initialize based on initial settings.
        for (Cursor c : cursors) {
          c.initCursorPos();
        }

        while (!done) {
          synchronized (newCursors) {
            cursors.addAll(newCursors);
            newCursors.clear();
          }

          synchronized (rmCursors) {
            cursors.removeAll(rmCursors);
            rmCursors.clear();
          }

          boolean madeProgress = false;
          for (Cursor c : cursors) {
            if (c.tailBody()) {
              madeProgress = true;
            }
          }

          if (!madeProgress) {
            Clock.sleep(sleepTime);
          }
        }
      } catch (InterruptedException e) {
        LOG.error("tail unexpected interrupted: " + e.getMessage(), e);
      } finally {
        LOG.info("Tail has exited");
      }

    }
  }

  /**
   * Add another file Cursor to tail concurrently.
   */
  synchronized void addCursor(Cursor cursor) {
    Preconditions.checkArgument(cursor != null);

    if (thd == null) {
      cursors.add(cursor);
    } else {
      synchronized (newCursors) {
        newCursors.add(cursor);
      }
    }

  }

  /**
   * Remove an existing cursor to tail.
   */
  synchronized public void removeCursor(Cursor cursor) {
    Preconditions.checkArgument(cursor != null);
    if (thd == null) {
      cursors.remove(cursor);
    } else {

      synchronized (rmCursors) {
        rmCursors.add(cursor);
      }
    }

  }

  @Override
  public void close() throws IOException {
    synchronized (this) {
      done = true;
      thd = null;
    }
  }

  /**
   * This function will block when the end of all the files it is trying to tail
   * is reached.
   */
  @Override
  public Event next() throws IOException {
    try {
      while (!done) {
        // This blocks on the synchronized queue until a new event arrives.
        Event e = sync.poll(100, TimeUnit.MILLISECONDS);
        if (e == null)
          continue; // nothing there, retry.
        updateEventProcessingStats(e);
        return e;
      }
      return null; // closed
    } catch (InterruptedException e1) {
      LOG.warn("next unexpectedly interrupted :" + e1.getMessage(), e1);
      throw new IOException(e1.getMessage());
    }
  }

  @Override
  synchronized public void open() throws IOException {
    if (thd != null) {
      throw new IllegalStateException("Attempted to open tail source twice!");
    }
    thd = new TailThread();
    thd.start();
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        if (argv.length != 1 && argv.length != 2) {
          throw new IllegalArgumentException(
              "usage: tail(filename, [startFromEnd]) ");
        }
        boolean startFromEnd = false;
        if (argv.length == 2) {
          startFromEnd = Boolean.parseBoolean(argv[1]);
        }
        return new TailSource(new File(argv[0]), 0, FlumeConfiguration.get()
            .getTailPollPeriod(), startFromEnd);
      }
    };
  }

  public static SourceBuilder multiTailBuilder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length >= 1,
            "usage: multitail(file1[, file2[, ...]]) ");
        boolean startFromEnd = false;
        long pollPeriod = FlumeConfiguration.get().getTailPollPeriod();
        TailSource src = null;
        for (int i = 0; i < argv.length; i++) {
          if (src == null) {
            src = new TailSource(new File(argv[i]), 0, pollPeriod, startFromEnd);
          } else {
            src.addCursor(new Cursor(src.sync, new File(argv[i])));
          }
        }
        return src;
      }
    };
  }

}
