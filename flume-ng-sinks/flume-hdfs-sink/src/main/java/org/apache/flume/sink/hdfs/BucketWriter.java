
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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.flume.sink.hdfs.HDFSEventSink.WriterCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Internal API intended for HDFSSink use.
 * This class does file rolling and handles file formats and serialization.
 * Only the public methods in this class are thread safe.
 */
class BucketWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(BucketWriter.class);

  static final String IN_USE_EXT = ".tmp";
  /**
   * This lock ensures that only one thread can open a file at a time.
   */
  private static final Integer staticLock = new Integer(1);

  private final HDFSWriter writer;
  private final FlumeFormatter formatter;
  private final long rollInterval;
  private final long rollSize;
  private final long rollCount;
  private final long batchSize;
  private final CompressionCodec codeC;
  private final CompressionType compType;
  private final Context context;
  private final ScheduledExecutorService timedRollerPool;
  private final UserGroupInformation user;

  private final AtomicLong fileExtensionCounter;

  private long eventCounter;
  private long processSize;

  private FileSystem fileSystem;

  private volatile String filePath;
  private volatile String fileSuffix;
  private volatile String bucketPath;
  private volatile long batchCounter;
  private volatile boolean isOpen;
  private volatile ScheduledFuture<Void> timedRollFuture;
  private SinkCounter sinkCounter;
  private final WriterCallback onIdleCallback;
  private final int idleTimeout;
  private volatile ScheduledFuture<Void> idleFuture;

  private Clock clock = new SystemClock();

  // flag that the bucket writer was closed due to idling and thus shouldn't be
  // reopened. Not ideal, but avoids internals of owners
  protected boolean idleClosed = false;

  BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
      Context context, String filePath, String fileSuffix, CompressionCodec codeC,
      CompressionType compType, HDFSWriter writer, FlumeFormatter formatter,
      ScheduledExecutorService timedRollerPool, UserGroupInformation user,
      SinkCounter sinkCounter, int idleTimeout, WriterCallback onIdleCallback) {
    this.rollInterval = rollInterval;
    this.rollSize = rollSize;
    this.rollCount = rollCount;
    this.batchSize = batchSize;
    this.context = context;
    this.filePath = filePath;
    this.fileSuffix = fileSuffix;
    this.codeC = codeC;
    this.compType = compType;
    this.writer = writer;
    this.formatter = formatter;
    this.timedRollerPool = timedRollerPool;
    this.user = user;
    this.sinkCounter = sinkCounter;
    this.onIdleCallback = onIdleCallback;
    this.idleTimeout = idleTimeout;

    fileExtensionCounter = new AtomicLong(clock.currentTimeMillis());

    isOpen = false;
    writer.configure(context);
  }

  /**
   * Allow methods to act as another user (typically used for HDFS Kerberos)
   * @param <T>
   * @param action
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private <T> T runPrivileged(final PrivilegedExceptionAction<T> action)
      throws IOException, InterruptedException {

    if (user != null) {
      return user.doAs(action);
    } else {
      try {
        return action.run();
      } catch (IOException ex) {
        throw ex;
      } catch (InterruptedException ex) {
        throw ex;
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException("Unexpected exception.", ex);
      }
    }
  }

  /**
   * Clear the class counters
   */
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    batchCounter = 0;
  }

  /**
   * open() is called by append()
   * @throws IOException
   */
  private void open() throws IOException, InterruptedException {
    runPrivileged(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        doOpen();
        return null;
      }
    });
  }

  /**
   * doOpen() must only be called by open()
   * @throws IOException
   */
  private void doOpen() throws IOException {
    if ((filePath == null) || (writer == null) || (formatter == null)) {
      throw new IOException("Invalid file settings");
    }

    Configuration config = new Configuration();
    // disable FileSystem JVM shutdown hook
    config.setBoolean("fs.automatic.close", false);

    // Hadoop is not thread safe when doing certain RPC operations,
    // including getFileSystem(), when running under Kerberos.
    // open() must be called by one thread at a time in the JVM.
    // NOTE: tried synchronizing on the underlying Kerberos principal previously
    // which caused deadlocks. See FLUME-1231.
    synchronized (staticLock) {
      try {
        long counter = fileExtensionCounter.incrementAndGet();
        if (codeC == null) {
          bucketPath = filePath + "." + counter;
          // FLUME-1645 - add suffix if specified
          if (fileSuffix != null && fileSuffix.length() > 0) {
            bucketPath += fileSuffix;
          }
          // Need to get reference to FS using above config before underlying
          // writer does in order to avoid shutdown hook & IllegalStateExceptions
          fileSystem = new Path(bucketPath).getFileSystem(config);
          LOG.info("Creating " + bucketPath + IN_USE_EXT);
          writer.open(bucketPath + IN_USE_EXT, formatter);
        } else {
          bucketPath = filePath + "." + counter
              + codeC.getDefaultExtension();
          // need to get reference to FS before writer does to avoid shutdown hook
          fileSystem = new Path(bucketPath).getFileSystem(config);
          LOG.info("Creating " + bucketPath + IN_USE_EXT);
          writer.open(bucketPath + IN_USE_EXT, codeC, compType, formatter);
        }
      } catch (Exception ex) {
        sinkCounter.incrementConnectionFailedCount();
        if (ex instanceof IOException) {
          throw (IOException) ex;
        } else {
          throw Throwables.propagate(ex);
        }
      }
    }
    sinkCounter.incrementConnectionCreatedCount();
    resetCounters();

    // if time-based rolling is enabled, schedule the roll
    if (rollInterval > 0) {
      Callable<Void> action = new Callable<Void>() {
        public Void call() throws Exception {
          LOG.debug("Rolling file ({}): Roll scheduled after {} sec elapsed.",
              bucketPath + IN_USE_EXT, rollInterval);
          try {
            close();
          } catch(Throwable t) {
            LOG.error("Unexpected error", t);
          }
          return null;
        }
      };
      timedRollFuture = timedRollerPool.schedule(action, rollInterval,
          TimeUnit.SECONDS);
    }

    isOpen = true;
  }

  /**
   * Close the file handle and rename the temp file to the permanent filename.
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions.
   * @throws IOException On failure to rename if temp file exists.
   */
  public synchronized void close() throws IOException, InterruptedException {
    flush();
    runPrivileged(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        doClose();
        return null;
      }
    });
  }

  /**
   * doClose() must only be called by close()
   * @throws IOException
   */
  private void doClose() throws IOException {
    LOG.debug("Closing {}", bucketPath + IN_USE_EXT);
    if (isOpen) {
      try {
        writer.close(); // could block
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        LOG.warn("failed to close() HDFSWriter for file (" + bucketPath +
            IN_USE_EXT + "). Exception follows.", e);
        sinkCounter.incrementConnectionFailedCount();
      }
      isOpen = false;
    } else {
      LOG.info("HDFSWriter is already closed: {}", bucketPath + IN_USE_EXT);
    }

    // NOTE: timed rolls go through this codepath as well as other roll types
    if (timedRollFuture != null && !timedRollFuture.isDone()) {
      timedRollFuture.cancel(false); // do not cancel myself if running!
      timedRollFuture = null;
    }

    if(idleFuture != null && !idleFuture.isDone()) {
      idleFuture.cancel(false);
      idleFuture = null;
    }

    if (bucketPath != null && fileSystem != null) {
      renameBucket(); // could block or throw IOException
      fileSystem = null;
    }
  }

  /**
   * flush the data
   */
  public synchronized void flush() throws IOException, InterruptedException {
    if (!isBatchComplete()) {
      runPrivileged(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          doFlush();
          return null;
        }
      });

      if(idleTimeout > 0) {
        // if the future exists and couldn't be cancelled, that would mean it has already run
        // or been cancelled
        if(idleFuture == null || idleFuture.cancel(false)) {
          Callable<Void> idleAction = new Callable<Void>() {
            public Void call() throws Exception {
              try {
                LOG.info("Closing idle bucketWriter {}", filePath);
                idleClosed = true;
                close();
                if(onIdleCallback != null)
                  onIdleCallback.run(filePath);
              } catch(Throwable t) {
                LOG.error("Unexpected error", t);
              }
              return null;
            }
          };
          idleFuture = timedRollerPool.schedule(idleAction, idleTimeout,
              TimeUnit.SECONDS);
        }
      }
    }
  }

  /**
   * doFlush() must only be called by flush()
   * @throws IOException
   */
  private void doFlush() throws IOException {
    writer.sync(); // could block
    batchCounter = 0;
  }

  /**
   * Open file handles, write data, update stats, handle file rolling and
   * batching / flushing. <br />
   * If the write fails, the file is implicitly closed and then the IOException
   * is rethrown. <br />
   * We rotate before append, and not after, so that the active file rolling
   * mechanism will never roll an empty file. This also ensures that the file
   * creation time reflects when the first event was written.
   */
  public synchronized void append(Event event) throws IOException, InterruptedException {
    if (!isOpen) {
      if(idleClosed) {
        throw new IOException("This bucket writer was closed due to idling and this handle " +
            "is thus no longer valid");
      }
      open();
    }

    // check if it's time to rotate the file
    if (shouldRotate()) {
      close();
      open();
    }

    // write the event
    try {
      sinkCounter.incrementEventDrainAttemptCount();
      writer.append(event, formatter); // could block
    } catch (IOException e) {
      LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" +
          bucketPath + IN_USE_EXT + ") and rethrowing exception.",
          e.getMessage());
      try {
        close();
      } catch (IOException e2) {
        LOG.warn("Caught IOException while closing file (" +
             bucketPath + IN_USE_EXT + "). Exception follows.", e2);
      }
      throw e;
    }

    // update statistics
    processSize += event.getBody().length;
    eventCounter++;
    batchCounter++;

    if (batchCounter == batchSize) {
      flush();
    }
  }

  /**
   * check if time to rotate the file
   */
  private boolean shouldRotate() {
    boolean doRotate = false;

    if ((rollCount > 0) && (rollCount <= eventCounter)) {
      LOG.debug("rolling: rollCount: {}, events: {}", rollCount, eventCounter);
      doRotate = true;
    }

    if ((rollSize > 0) && (rollSize <= processSize)) {
      LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processSize);
      doRotate = true;
    }

    return doRotate;
  }

  /**
   * Rename bucketPath file from .tmp to permanent location.
   */
  private void renameBucket() throws IOException {
    Path srcPath = new Path(bucketPath + IN_USE_EXT);
    Path dstPath = new Path(bucketPath);

    if(fileSystem.exists(srcPath)) { // could block
      LOG.info("Renaming " + srcPath + " to " + dstPath);
      fileSystem.rename(srcPath, dstPath); // could block
    }
  }

  @Override
  public String toString() {
    return "[ " + this.getClass().getSimpleName() + " filePath = " + filePath +
        ", bucketPath = " + bucketPath + " ]";
  }

  private boolean isBatchComplete() {
    return (batchCounter == 0);
  }

  void setClock(Clock clock) {
      this.clock = clock;
  }
}
