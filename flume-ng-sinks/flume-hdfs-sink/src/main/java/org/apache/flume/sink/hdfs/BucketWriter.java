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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.hdfs.HDFSEventSink.WriterCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal API intended for HDFSSink use.
 * This class does file rolling and handles file formats and serialization.
 * Only the public methods in this class are thread safe.
 */
class BucketWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(BucketWriter.class);

  /**
   * This lock ensures that only one thread can open a file at a time.
   */
  private static final Integer staticLock = new Integer(1);
  private Method isClosedMethod = null;

  private HDFSWriter writer;
  private final long rollInterval;
  private final long rollSize;
  private final long rollCount;
  private final long batchSize;
  private final CompressionCodec codeC;
  private final CompressionType compType;
  private final ScheduledExecutorService timedRollerPool;
  private final PrivilegedExecutor proxyUser;

  private final AtomicLong fileExtensionCounter;

  private long eventCounter;
  private long processSize;

  private FileSystem fileSystem;

  private volatile String filePath;
  private volatile String fileName;
  private volatile String inUsePrefix;
  private volatile String inUseSuffix;
  private volatile String fileSuffix;
  private volatile String bucketPath;
  private volatile String targetPath;
  private volatile long batchCounter;
  private volatile boolean isOpen;
  private volatile boolean isUnderReplicated;
  private volatile int consecutiveUnderReplRotateCount = 0;
  private volatile ScheduledFuture<Void> timedRollFuture;
  private SinkCounter sinkCounter;
  private final int idleTimeout;
  private volatile ScheduledFuture<Void> idleFuture;
  private final WriterCallback onCloseCallback;
  private final String onCloseCallbackPath;
  private final long callTimeout;
  private final ExecutorService callTimeoutPool;
  private final int maxConsecUnderReplRotations = 30; // make this config'able?

  private boolean mockFsInjected = false;

  private final long retryInterval;
  private final int maxRenameTries;

  // flag that the bucket writer was closed due to idling and thus shouldn't be
  // reopened. Not ideal, but avoids internals of owners
  protected boolean closed = false;
  AtomicInteger renameTries = new AtomicInteger(0);

  BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
      Context context, String filePath, String fileName, String inUsePrefix,
      String inUseSuffix, String fileSuffix, CompressionCodec codeC,
      CompressionType compType, HDFSWriter writer,
      ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
      SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
      String onCloseCallbackPath, long callTimeout,
      ExecutorService callTimeoutPool, long retryInterval,
      int maxCloseTries) {
    this(rollInterval, rollSize, rollCount, batchSize,
            context, filePath, fileName, inUsePrefix,
            inUseSuffix, fileSuffix, codeC,
            compType, writer,
            timedRollerPool, proxyUser,
            sinkCounter, idleTimeout, onCloseCallback,
            onCloseCallbackPath, callTimeout,
            callTimeoutPool, retryInterval,
            maxCloseTries, new SystemClock());
  }

  BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
           Context context, String filePath, String fileName, String inUsePrefix,
           String inUseSuffix, String fileSuffix, CompressionCodec codeC,
           CompressionType compType, HDFSWriter writer,
           ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
           SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
           String onCloseCallbackPath, long callTimeout,
           ExecutorService callTimeoutPool, long retryInterval,
           int maxCloseTries, Clock clock) {
    this.rollInterval = rollInterval;
    this.rollSize = rollSize;
    this.rollCount = rollCount;
    this.batchSize = batchSize;
    this.filePath = filePath;
    this.fileName = fileName;
    this.inUsePrefix = inUsePrefix;
    this.inUseSuffix = inUseSuffix;
    this.fileSuffix = fileSuffix;
    this.codeC = codeC;
    this.compType = compType;
    this.writer = writer;
    this.timedRollerPool = timedRollerPool;
    this.proxyUser = proxyUser;
    this.sinkCounter = sinkCounter;
    this.idleTimeout = idleTimeout;
    this.onCloseCallback = onCloseCallback;
    this.onCloseCallbackPath = onCloseCallbackPath;
    this.callTimeout = callTimeout;
    this.callTimeoutPool = callTimeoutPool;
    fileExtensionCounter = new AtomicLong(clock.currentTimeMillis());

    this.retryInterval = retryInterval;
    this.maxRenameTries = maxCloseTries;
    isOpen = false;
    isUnderReplicated = false;
    this.writer.configure(context);
  }

  @VisibleForTesting
  void setFileSystem(FileSystem fs) {
    this.fileSystem = fs;
    mockFsInjected = true;
  }

  @VisibleForTesting
  void setMockStream(HDFSWriter dataWriter) {
    this.writer = dataWriter;
  }


  /**
   * Clear the class counters
   */
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    batchCounter = 0;
  }

  private Method getRefIsClosed() {
    try {
      return fileSystem.getClass().getMethod("isFileClosed",
        Path.class);
    } catch (Exception e) {
      LOG.info("isFileClosed() is not available in the version of the " +
               "distributed filesystem being used. " +
               "Flume will not attempt to re-close files if the close fails " +
               "on the first attempt");
      return null;
    }
  }

  private Boolean isFileClosed(FileSystem fs, Path tmpFilePath) throws Exception {
    return (Boolean)(isClosedMethod.invoke(fs, tmpFilePath));
  }

  /**
   * open() is called by append()
   * @throws IOException
   * @throws InterruptedException
   */
  private void open() throws IOException, InterruptedException {
    if ((filePath == null) || (writer == null)) {
      throw new IOException("Invalid file settings");
    }

    final Configuration config = new Configuration();
    // disable FileSystem JVM shutdown hook
    config.setBoolean("fs.automatic.close", false);

    // Hadoop is not thread safe when doing certain RPC operations,
    // including getFileSystem(), when running under Kerberos.
    // open() must be called by one thread at a time in the JVM.
    // NOTE: tried synchronizing on the underlying Kerberos principal previously
    // which caused deadlocks. See FLUME-1231.
    synchronized (staticLock) {
      checkAndThrowInterruptedException();

      try {
        long counter = fileExtensionCounter.incrementAndGet();

        String fullFileName = fileName + "." + counter;

        if (fileSuffix != null && fileSuffix.length() > 0) {
          fullFileName += fileSuffix;
        } else if (codeC != null) {
          fullFileName += codeC.getDefaultExtension();
        }

        bucketPath = filePath + "/" + inUsePrefix
          + fullFileName + inUseSuffix;
        targetPath = filePath + "/" + fullFileName;

        LOG.info("Creating " + bucketPath);
        callWithTimeout(new CallRunner<Void>() {
          @Override
          public Void call() throws Exception {
            if (codeC == null) {
              // Need to get reference to FS using above config before underlying
              // writer does in order to avoid shutdown hook &
              // IllegalStateExceptions
              if (!mockFsInjected) {
                fileSystem = new Path(bucketPath).getFileSystem(config);
              }
              writer.open(bucketPath);
            } else {
              // need to get reference to FS before writer does to
              // avoid shutdown hook
              if (!mockFsInjected) {
                fileSystem = new Path(bucketPath).getFileSystem(config);
              }
              writer.open(bucketPath, codeC, compType);
            }
            return null;
          }
        });
      } catch (Exception ex) {
        sinkCounter.incrementConnectionFailedCount();
        if (ex instanceof IOException) {
          throw (IOException) ex;
        } else {
          throw Throwables.propagate(ex);
        }
      }
    }
    isClosedMethod = getRefIsClosed();
    sinkCounter.incrementConnectionCreatedCount();
    resetCounters();

    // if time-based rolling is enabled, schedule the roll
    if (rollInterval > 0) {
      Callable<Void> action = new Callable<Void>() {
        public Void call() throws Exception {
          LOG.debug("Rolling file ({}): Roll scheduled after {} sec elapsed.",
              bucketPath, rollInterval);
          try {
            // Roll the file and remove reference from sfWriters map.
            close(true);
          } catch (Throwable t) {
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
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions. This
   * method will not cause the bucket writer to be dereferenced from the HDFS
   * sink that owns it. This method should be used only when size or count
   * based rolling closes this file.
   * @throws IOException On failure to rename if temp file exists.
   * @throws InterruptedException
   */
  public synchronized void close() throws IOException, InterruptedException {
    close(false);
  }

  private CallRunner<Void> createCloseCallRunner() {
    return new CallRunner<Void>() {
      private final HDFSWriter localWriter = writer;
      @Override
      public Void call() throws Exception {
        localWriter.close(); // could block
        return null;
      }
    };
  }

  private Callable<Void> createScheduledRenameCallable() {

    return new Callable<Void>() {
      private final String path = bucketPath;
      private final String finalPath = targetPath;
      private FileSystem fs = fileSystem;
      private int renameTries = 1; // one attempt is already done

      @Override
      public Void call() throws Exception {
        if (renameTries >= maxRenameTries) {
          LOG.warn("Unsuccessfully attempted to rename " + path + " " +
              maxRenameTries + " times. File may still be open.");
          return null;
        }
        renameTries++;
        try {
          renameBucket(path, finalPath, fs);
        } catch (Exception e) {
          LOG.warn("Renaming file: " + path + " failed. Will " +
              "retry again in " + retryInterval + " seconds.", e);
          timedRollerPool.schedule(this, retryInterval, TimeUnit.SECONDS);
          return null;
        }
        return null;
      }
    };
  }

  /**
   * Tries to start the lease recovery process for the current bucketPath
   * if the fileSystem is DistributedFileSystem.
   * Catches and logs the IOException.
   */
  private synchronized void recoverLease() {
    if (bucketPath != null && fileSystem instanceof DistributedFileSystem) {
      try {
        LOG.debug("Starting lease recovery for {}", bucketPath);
        ((DistributedFileSystem) fileSystem).recoverLease(new Path(bucketPath));
      } catch (IOException ex) {
        LOG.warn("Lease recovery failed for {}", bucketPath, ex);
      }
    }
  }

  /**
   * Close the file handle and rename the temp file to the permanent filename.
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions.
   * @throws IOException On failure to rename if temp file exists.
   * @throws InterruptedException
   */
  public synchronized void close(boolean callCloseCallback)
      throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    try {
      flush();
    } catch (IOException e) {
      LOG.warn("pre-close flush failed", e);
    }

    LOG.info("Closing {}", bucketPath);
    CallRunner<Void> closeCallRunner = createCloseCallRunner();
    if (isOpen) {
      try {
        callWithTimeout(closeCallRunner);
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        LOG.warn("failed to close() HDFSWriter for file (" + bucketPath +
                 "). Exception follows.", e);
        sinkCounter.incrementConnectionFailedCount();
        // starting lease recovery process, see FLUME-3080
        recoverLease();
      }
      isOpen = false;
    } else {
      LOG.info("HDFSWriter is already closed: {}", bucketPath);
    }

    // NOTE: timed rolls go through this codepath as well as other roll types
    if (timedRollFuture != null && !timedRollFuture.isDone()) {
      timedRollFuture.cancel(false); // do not cancel myself if running!
      timedRollFuture = null;
    }

    if (idleFuture != null && !idleFuture.isDone()) {
      idleFuture.cancel(false); // do not cancel myself if running!
      idleFuture = null;
    }

    if (bucketPath != null && fileSystem != null) {
      // could block or throw IOException
      try {
        renameBucket(bucketPath, targetPath, fileSystem);
      } catch (Exception e) {
        LOG.warn("failed to rename() file (" + bucketPath +
                 "). Exception follows.", e);
        sinkCounter.incrementConnectionFailedCount();
        final Callable<Void> scheduledRename = createScheduledRenameCallable();
        timedRollerPool.schedule(scheduledRename, retryInterval, TimeUnit.SECONDS);
      }
    }
    if (callCloseCallback) {
      runCloseAction();
      closed = true;
    }
  }

  /**
   * flush the data
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void flush() throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    if (!isBatchComplete()) {
      doFlush();

      if (idleTimeout > 0) {
        // if the future exists and couldn't be cancelled, that would mean it has already run
        // or been cancelled
        if (idleFuture == null || idleFuture.cancel(false)) {
          Callable<Void> idleAction = new Callable<Void>() {
            public Void call() throws Exception {
              LOG.info("Closing idle bucketWriter {} at {}", bucketPath,
                       System.currentTimeMillis());
              if (isOpen) {
                close(true);
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

  private void runCloseAction() {
    try {
      if (onCloseCallback != null) {
        onCloseCallback.run(onCloseCallbackPath);
      }
    } catch (Throwable t) {
      LOG.error("Unexpected error", t);
    }
  }

  /**
   * doFlush() must only be called by flush()
   * @throws IOException
   */
  private void doFlush() throws IOException, InterruptedException {
    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        writer.sync(); // could block
        return null;
      }
    });
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
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void append(final Event event)
          throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    // If idleFuture is not null, cancel it before we move forward to avoid a
    // close call in the middle of the append.
    if (idleFuture != null) {
      idleFuture.cancel(false);
      // There is still a small race condition - if the idleFuture is already
      // running, interrupting it can cause HDFS close operation to throw -
      // so we cannot interrupt it while running. If the future could not be
      // cancelled, it is already running - wait for it to finish before
      // attempting to write.
      if (!idleFuture.isDone()) {
        try {
          idleFuture.get(callTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
          LOG.warn("Timeout while trying to cancel closing of idle file. Idle" +
                   " file close may have failed", ex);
        } catch (Exception ex) {
          LOG.warn("Error while trying to cancel closing of idle file. ", ex);
        }
      }
      idleFuture = null;
    }

    // If the bucket writer was closed due to roll timeout or idle timeout,
    // force a new bucket writer to be created. Roll count and roll size will
    // just reuse this one
    if (!isOpen) {
      if (closed) {
        throw new BucketClosedException("This bucket writer was closed and " +
          "this handle is thus no longer valid");
      }
      if (checkQuota(event)) {
        throw new IOException("This bucket has reached the expected quota"); 
      }
      open();
    }

    // check if it's time to rotate the file
    if (shouldRotate()) {
      boolean doRotate = true;

      if (isUnderReplicated) {
        if (maxConsecUnderReplRotations > 0 &&
            consecutiveUnderReplRotateCount >= maxConsecUnderReplRotations) {
          doRotate = false;
          if (consecutiveUnderReplRotateCount == maxConsecUnderReplRotations) {
            LOG.error("Hit max consecutive under-replication rotations ({}); " +
                "will not continue rolling files under this path due to " +
                "under-replication", maxConsecUnderReplRotations);
          }
        } else {
          LOG.warn("Block Under-replication detected. Rotating file.");
        }
        consecutiveUnderReplRotateCount++;
      } else {
        consecutiveUnderReplRotateCount = 0;
      }

      if (doRotate) {
        close();
        open();
      }
    }

    // write the event
    try {
      sinkCounter.incrementEventDrainAttemptCount();
      callWithTimeout(new CallRunner<Void>() {
        @Override
        public Void call() throws Exception {
          if (checkQuota(event)) {
            throw new IOException("This bucket has reached the expected quota"); 
          }
          writer.append(event); // could block
          return null;
        }
      });
    } catch (IOException e) {
      LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" +
          bucketPath + ") and rethrowing exception.",
          e.getMessage());
      try {
        close(true);
      } catch (IOException e2) {
        LOG.warn("Caught IOException while closing file (" +
             bucketPath + "). Exception follows.", e2);
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

    if (writer.isUnderReplicated()) {
      this.isUnderReplicated = true;
      doRotate = true;
    } else {
      this.isUnderReplicated = false;
    }

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
  // When this bucket writer is rolled based on rollCount or
  // rollSize, the same instance is reused for the new file. But if
  // the previous file was not closed/renamed,
  // the bucket writer fields no longer point to it and hence need
  // to be passed in from the thread attempting to close it. Even
  // when the bucket writer is closed due to close timeout,
  // this method can get called from the scheduled thread so the
  // file gets closed later - so an implicit reference to this
  // bucket writer would still be alive in the Callable instance.
  private void renameBucket(String bucketPath, String targetPath, final FileSystem fs)
      throws IOException, InterruptedException {
    if (bucketPath.equals(targetPath)) {
      return;
    }

    final Path srcPath = new Path(bucketPath);
    final Path dstPath = new Path(targetPath);

    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        if (fs.exists(srcPath)) { // could block
          LOG.info("Renaming " + srcPath + " to " + dstPath);
          renameTries.incrementAndGet();
          fs.rename(srcPath, dstPath); // could block
        }
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return "[ " + this.getClass().getSimpleName() + " targetPath = " + targetPath +
        ", bucketPath = " + bucketPath + " ]";
  }

  private boolean isBatchComplete() {
    return (batchCounter == 0);
  }

  /**
   * This method if the current thread has been interrupted and throws an
   * exception.
   * @throws InterruptedException
   */
  private static void checkAndThrowInterruptedException()
          throws InterruptedException {
    if (Thread.currentThread().interrupted()) {
      throw new InterruptedException("Timed out before HDFS call was made. "
              + "Your hdfs.callTimeout might be set too low or HDFS calls are "
              + "taking too long.");
    }
  }
  
  private boolean checkQuota(Event event) {
    if (filePath != null) {
      final Path quotaPath = new Path(filePath);
      final Configuration config = new Configuration();
      config.setBoolean("fs.automatic.close", false); 

      ContentSummary cSumm;
      try {
        cSumm = fsCheckQuota(quotaPath.getFileSystem(config),quotaPath);
        long quota = cSumm.getSpaceQuota();
        long spaceConsumed = cSumm.getSpaceConsumed();
        long block = quotaPath.getFileSystem(config).getDefaultBlockSize(quotaPath);
        long quotaCondition = block + spaceConsumed + event.getBody().length;
          
        if (quota != -1 && (quotaCondition >= quota)) {
          return Boolean.TRUE;
        }
      } catch (IOException e) {
        LOG.error("Error on getting quota from " + filePath,e);
      }
    }
    return Boolean.FALSE;
  }

  /**
   * Execute the callable on a separate thread and wait for the completion
   * for the specified amount of time in milliseconds. In case of timeout
   * cancel the callable and throw an IOException
   */
  private <T> T callWithTimeout(final CallRunner<T> callRunner)
      throws IOException, InterruptedException {
    Future<T> future = callTimeoutPool.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return proxyUser.execute(new PrivilegedExceptionAction<T>() {
          @Override
          public T run() throws Exception {
            return callRunner.call();
          }
        });
      }
    });
    try {
      if (callTimeout > 0) {
        return future.get(callTimeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      sinkCounter.incrementConnectionFailedCount();
      throw new IOException("Callable timed out after " +
        callTimeout + " ms" + " on file: " + bucketPath, eT);
    } catch (ExecutionException e1) {
      sinkCounter.incrementConnectionFailedCount();
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else {
        throw new RuntimeException(e1);
      }
    } catch (CancellationException ce) {
      throw new InterruptedException(
        "Blocked callable interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      throw ex;
    }
  }

  /**
   * Simple interface whose <tt>call</tt> method is called by
   * {#callWithTimeout} in a new thread inside a
   * {@linkplain java.security.PrivilegedExceptionAction#run()} call.
   * @param <T>
   */
  private interface CallRunner<T> {
    T call() throws Exception;
  }

}
