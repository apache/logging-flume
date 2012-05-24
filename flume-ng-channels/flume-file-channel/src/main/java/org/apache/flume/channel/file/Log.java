/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Stores FlumeEvents on disk and pointers to the events in a in memory queue.
 * Once a log object is created the replay method should be called to reconcile
 * the on disk write ahead log with the last checkpoint of the queue.
 */
class Log {
  public static final String PREFIX = "log-";
  private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);
  private static final int MIN_NUM_LOGS = 2;
  private static final String FILE_LOCK = "in_use.lock";
  // for reader
  private final Map<Integer, LogFile.RandomReader> idLogFileMap = Collections
      .synchronizedMap(new HashMap<Integer, LogFile.RandomReader>());
  private final AtomicInteger nextFileID = new AtomicInteger(0);
  private final File checkpointDir;
  private final File[] logDirs;
  private final BackgroundWorker worker;
  private final int queueSize;
  private final AtomicReferenceArray<LogFile.Writer> logFiles;

  private volatile boolean open;
  private AtomicReference<Checkpoint> checkpoint;
  private Checkpoint checkpointA;
  private Checkpoint checkpointB;
  private FlumeEventQueue queue;
  private long checkpointInterval;
  private long maxFileSize;
  private final Map<String, FileLock> locks;

  Log(long checkpointInterval, long maxFileSize, int queueSize,
      File checkpointDir, File... logDirs) throws IOException {
    Preconditions.checkArgument(checkpointInterval > 0,
        "checkpointInterval <= 0");
    Preconditions.checkArgument(queueSize > 0, "queueSize <= 0");
    Preconditions.checkArgument(maxFileSize > 0, "maxFileSize <= 0");
    Preconditions.checkNotNull(checkpointDir, "checkpointDir");
    Preconditions.checkArgument(
        checkpointDir.isDirectory() || checkpointDir.mkdirs(), "CheckpointDir "
            + checkpointDir + " could not be created");
    Preconditions.checkNotNull(logDirs, "logDirs");
    Preconditions.checkArgument(logDirs.length > 0, "logDirs empty");
    for (File logDir : logDirs) {
      Preconditions.checkArgument(logDir.isDirectory() || logDir.mkdirs(),
          "LogDir " + logDir + " could not be created");
    }
    locks = Maps.newHashMap();
    try {
      lock(checkpointDir);
      for (File logDir : logDirs) {
        lock(logDir);
      }
    } catch(IOException e) {
      unlock(checkpointDir);
      for (File logDir : logDirs) {
        unlock(logDir);
      }
      throw e;
    }
    open = false;
    this.checkpointInterval = checkpointInterval;
    this.maxFileSize = maxFileSize;
    this.queueSize = queueSize;
    this.checkpointDir = checkpointDir;
    this.logDirs = logDirs;
    logFiles = new AtomicReferenceArray<LogFile.Writer>(this.logDirs.length);
    worker = new BackgroundWorker(this);
    worker.setName("Log-BackgroundWorker");
    worker.setDaemon(true);
    worker.start();
  }

  /**
   * Read checkpoint and data files from disk replaying them to the state
   * directly before the shutdown or crash.
   * @throws IOException
   */
  synchronized void replay() throws IOException {
    Preconditions.checkState(!open, "Cannot replay after Log as been opened");
    try {
      /*
       * First we are going to look through the data directories
       * and find all log files. We will store the highest file id
       * (at the end of the filename) we find and use that when we
       * create additional log files.
       *
       * Also store up the list of files so we can replay them later.
       */
      LOGGER.info("Replay started");
      nextFileID.set(0);
      List<File> dataFiles = Lists.newArrayList();
      for (File logDir : logDirs) {
        for (File file : LogUtils.getLogs(logDir)) {
          int id = LogUtils.getIDForFile(file);
          dataFiles.add(file);
          nextFileID.set(Math.max(nextFileID.get(), id));
          idLogFileMap.put(id, new LogFile.RandomReader(new File(logDir, PREFIX
              + id)));
        }
      }
      LOGGER.info("Found NextFileID " + nextFileID +
          ", from " + Arrays.toString(logDirs));

      /*
       * sort the data files by file id so we can replay them by file id
       * which should approximately give us sequential events
       */
      LogUtils.sort(dataFiles);

      /*
       * Read the checkpoint (in memory queue) from one of two alternating
       * locations. We will read the last one written to disk.
       */
      checkpointA = new Checkpoint(new File(checkpointDir, "chkpt-A"),
          queueSize);
      checkpointB = new Checkpoint(new File(checkpointDir, "chkpt-B"),
          queueSize);
      if (checkpointA.getTimestamp() > checkpointB.getTimestamp()) {
        try {
          LOGGER.info("Reading checkpoint A " + checkpointA.getFile());
          // read from checkpoint A, write to B
          queue = checkpointA.read();
          checkpoint = new AtomicReference<Checkpoint>(checkpointB);
        } catch (EOFException e) {
          LOGGER.info("EOF reading from A", e);
          // read from checkpoint B, write to A
          queue = checkpointB.read();
          checkpoint = new AtomicReference<Checkpoint>(checkpointA);
        }
      } else if (checkpointB.getTimestamp() > checkpointA.getTimestamp()) {
        try {
          LOGGER.info("Reading checkpoint B " + checkpointB.getFile());
          // read from checkpoint B, write to A
          queue = checkpointB.read();
          checkpoint = new AtomicReference<Checkpoint>(checkpointA);
        } catch (EOFException e) {
          LOGGER.info("EOF reading from B", e);
          // read from checkpoint A, write to B
          queue = checkpointA.read();
          checkpoint = new AtomicReference<Checkpoint>(checkpointB);
        }
      } else {
        LOGGER.info("Starting checkpoint from scratch");
        queue = new FlumeEventQueue(queueSize);
        checkpoint = new AtomicReference<Checkpoint>(checkpointA);
      }

      long ts = checkpoint.get().getTimestamp();
      LOGGER.info("Last Checkpoint " + new Date(ts) +
          ", queue depth = " + queue.size());

      /*
       * We now have everything we need to actually replay the log files
       * the queue, the timestamp the queue was written to disk, and
       * the list of data files.
       */
      ReplayHandler replayHandler = new ReplayHandler(queue,
          checkpoint.get().getTimestamp());
      replayHandler.replayLog(dataFiles);
      for (int index = 0; index < logDirs.length; index++) {
        LOGGER.info("Rolling " + logDirs[index]);
        roll(index);
      }
      /*
       * Now that we have replayed, write the current queue to disk
       */
      writeCheckpoint();
      open = true;
    } catch (Exception ex) {
      LOGGER.error("Failed to initialize Log", ex);
    }
  }

  int getNextFileID() {
    Preconditions.checkState(open, "Log is closed");
    return nextFileID.get();
  }

  FlumeEventQueue getFlumeEventQueue() {
    Preconditions.checkState(open, "Log is closed");
    return queue;
  }

  /**
   * Return the FlumeEvent for an event pointer. This method is
   * non-transactional. It is assumed the client has obtained this
   * FlumeEventPointer via FlumeEventQueue.
   *
   * @param pointer
   * @return FlumeEventPointer
   * @throws IOException
   * @throws InterruptedException
   */
  FlumeEvent get(FlumeEventPointer pointer) throws IOException,
  InterruptedException {
    Preconditions.checkState(open, "Log is closed");
    int id = pointer.getFileID();
    LogFile.RandomReader logFile = idLogFileMap.get(id);
    Preconditions.checkNotNull(logFile, "LogFile is null for id " + id);
    return logFile.get(pointer.getOffset());
  }

  /**
   * Log a put of an event
   *
   * Synchronization not required as this method is atomic
   * @param transactionID
   * @param event
   * @return
   * @throws IOException
   */
  FlumeEventPointer put(long transactionID, Event event)
      throws IOException {
    Preconditions.checkState(open, "Log is closed");
    FlumeEvent flumeEvent = new FlumeEvent(event.getHeaders(), event.getBody());
    Put put = new Put(transactionID, flumeEvent);
    put.setTimestamp(System.currentTimeMillis());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(put);
    int logFileIndex = nextLogWriter(transactionID);
    if (logFiles.get(logFileIndex).isRollRequired(buffer)) {
      roll(logFileIndex, buffer);
    }
    boolean error = true;
    try {
      FlumeEventPointer ptr = logFiles.get(logFileIndex).put(buffer);
      error = false;
      return ptr;
    } finally {
      if (error) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log a take of an event, pointer points at the corresponding put
   *
   * Synchronization not required as this method is atomic
   * @param transactionID
   * @param pointer
   * @throws IOException
   */
  void take(long transactionID, FlumeEventPointer pointer)
      throws IOException {
    Preconditions.checkState(open, "Log is closed");
    Take take = new Take(transactionID, pointer.getOffset(),
        pointer.getFileID());
    take.setTimestamp(System.currentTimeMillis());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(take);
    int logFileIndex = nextLogWriter(transactionID);
    if (logFiles.get(logFileIndex).isRollRequired(buffer)) {
      roll(logFileIndex, buffer);
    }
    boolean error = true;
    try {
      logFiles.get(logFileIndex).take(buffer);
      error = false;
    } finally {
      if (error) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log a rollback of a transaction
   *
   * Synchronization not required as this method is atomic
   * @param transactionID
   * @throws IOException
   */
  void rollback(long transactionID) throws IOException {
    Preconditions.checkState(open, "Log is closed");
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Rolling back " + transactionID);
    }
    Rollback rollback = new Rollback(transactionID);
    rollback.setTimestamp(System.currentTimeMillis());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(rollback);
    int logFileIndex = nextLogWriter(transactionID);
    if (logFiles.get(logFileIndex).isRollRequired(buffer)) {
      roll(logFileIndex, buffer);
    }
    boolean error = true;
    try {
      logFiles.get(logFileIndex).rollback(buffer);
      error = false;
    } finally {
      if (error) {
        roll(logFileIndex);
      }
    }
  }

  /**
   * Log commit of put, we need to know which type of commit
   * so we know if the pointers corresponding to the events
   * should be added or removed from the flume queue. We
   * could infer but it's best to be explicit.
   *
   * Synchronization not required as this method is atomic
   * @param transactionID
   * @throws IOException
   * @throws InterruptedException
   */
  void commitPut(long transactionID) throws IOException,
  InterruptedException {
    Preconditions.checkState(open, "Log is closed");
    commit(transactionID, TransactionEventRecord.Type.PUT.get());
  }

  /**
   * Log commit of take, we need to know which type of commit
   * so we know if the pointers corresponding to the events
   * should be added or removed from the flume queue. We
   * could infer but it's best to be explicit.
   *
   * Synchronization not required as this method is atomic
   * @param transactionID
   * @throws IOException
   * @throws InterruptedException
   */
  void commitTake(long transactionID) throws IOException,
  InterruptedException {
    Preconditions.checkState(open, "Log is closed");
    commit(transactionID, TransactionEventRecord.Type.TAKE.get());
  }

  /**
   * Synchronization required since we do not want this
   * to be called during a checkpoint.
   */
  synchronized void close() {
    open = false;
    if (worker != null) {
      worker.shutdown();
    }
    if (logFiles != null) {
      for (int index = 0; index < logFiles.length(); index++) {
        logFiles.get(index).close();
      }
    }
    synchronized (idLogFileMap) {
      for(Integer logId : idLogFileMap.keySet()) {
        LogFile.RandomReader reader = idLogFileMap.get(logId);
        if(reader != null) {
          reader.close();
        }
      }
    }
    try {
      unlock(checkpointDir);
    } catch(IOException ex) {
      LOGGER.warn("Error unlocking " + checkpointDir, ex);
    }
    for (File logDir : logDirs) {
      try {
        unlock(logDir);
      } catch(IOException ex) {
        LOGGER.warn("Error unlocking " + logDir, ex);
      }
    }
  }

  synchronized void shutdownWorker() {
    Preconditions.checkNotNull(worker, "worker");
    worker.shutdown();
  }
  void setCheckpointInterval(long checkpointInterval) {
    this.checkpointInterval = checkpointInterval;
  }
  void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  /**
   * Synchronization not required as this method is atomic
   *
   * @param transactionID
   * @param type
   * @throws IOException
   */
  private void commit(long transactionID, short type) throws IOException {
    Commit commit = new Commit(transactionID, type);
    commit.setTimestamp(System.currentTimeMillis());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(commit);
    int logFileIndex = nextLogWriter(transactionID);
    if (logFiles.get(logFileIndex).isRollRequired(buffer)) {
      roll(logFileIndex, buffer);
    }
    boolean error = true;
    try {
      logFiles.get(logFileIndex).commit(buffer);
      error = false;
    } finally {
      if (error) {
        roll(logFileIndex);
      }
    }
  }
  /**
   * Atomic so not synchronization required.
   * @return
   */
  private int nextLogWriter(long transactionID) {
    return (int)Math.abs(transactionID % (long)logFiles.length());
  }
  /**
   * Unconditionally roll
   * Synchronization done internally
   * @param index
   * @throws IOException
   */
  private void roll(int index) throws IOException {
    roll(index, null);
  }
  /**
   * Roll a log if needed. Roll always occurs if the log at the index
   * does not exist (typically on startup), or buffer is null. Otherwise
   * LogFile.Writer.isRollRequired is checked again to ensure we don't
   * have threads pile up on this log resulting in multiple successive
   * rolls
   *
   * Synchronization required since both synchronized and unsynchronized
   * methods call this method.
   * @param index
   * @throws IOException
   */
  private synchronized void roll(int index, ByteBuffer buffer)
      throws IOException {
    LogFile.Writer oldLogFile = logFiles.get(index);
    // check to make sure a roll is actually required due to
    // the possibility of multiple writes waiting on lock
    if(oldLogFile == null || buffer == null ||
        oldLogFile.isRollRequired(buffer)) {
      try {
        LOGGER.info("Roll start " + logDirs[index]);
        int fileID = nextFileID.incrementAndGet();
        File file = new File(logDirs[index], PREFIX + fileID);
        Preconditions.checkState(!file.exists(), "File alread exists "  + file);
        Preconditions.checkState(file.createNewFile(), "File could not be created " + file);
        idLogFileMap.put(fileID, new LogFile.RandomReader(file));
        // writer from this point on will get new reference
        logFiles.set(index, new LogFile.Writer(file, fileID, maxFileSize));
        // close out old log
        if (oldLogFile != null) {
          oldLogFile.close();
        }
      } finally {
        LOGGER.info("Roll end");
      }
    }
  }
  /**
   * Write the current checkpoint object and then swap objects so that
   * the next checkpoint occurs on the other checkpoint directory.
   *
   * Synchronization required since both synchronized and unsynchronized
   * @throws IOException if we are unable to write the checkpoint out to disk
   */
  private synchronized void writeCheckpoint() throws IOException {
    synchronized (queue) {
      checkpoint.get().write(queue);
      if (!checkpoint.compareAndSet(checkpointA, checkpointB)) {
        Preconditions.checkState(checkpoint.compareAndSet(checkpointB,
            checkpointA));
      }
    }
  }
  /**
   * Synchronization not required as this is atomic
   * @return last time we successfully check pointed
   * @throws IOException if there is an io error reading the ts from disk
   */
  private long getLastCheckpoint() throws IOException {
    Preconditions.checkState(open, "Log is closed");
    return checkpoint.get().getTimestamp();
  }

  private void removeOldLogs() {
    Preconditions.checkState(open, "Log is closed");
    // we will find the smallest fileID currently in use and
    // won't delete any files with an id larger than the min
    Set<Integer> fileIDs = new TreeSet<Integer>(queue.getFileIDs());
    for (int index = 0; index < logDirs.length; index++) {
      fileIDs.add(logFiles.get(index).getFileID());
    }
    int minFileID = Collections.min(fileIDs);
    LOGGER.debug("Files currently in use: " + fileIDs);
    for(File logDir : logDirs) {
      List<File> logs = LogUtils.getLogs(logDir);
      // sort oldset to newest
      LogUtils.sort(logs);
      // ensure we always keep two logs per dir
      int size = logs.size() - MIN_NUM_LOGS;
      for (int index = 0; index < size; index++) {
        File logFile = logs.get(index);
        int logFileID = LogUtils.getIDForFile(logFile);
        if(logFileID < minFileID) {
          LogFile.RandomReader reader = idLogFileMap.remove(logFileID);
          if(reader != null) {
            reader.close();
          }
          LOGGER.info("Removing old log " + logFile +
              ", result = " + logFile.delete() + ", minFileID "
              + minFileID);
        }
      }
    }
  }
  /**
   * Lock storage to provide exclusive access.
   *
   * <p> Locking is not supported by all file systems.
   * E.g., NFS does not consistently support exclusive locks.
   *
   * <p> If locking is supported we guarantee exculsive access to the
   * storage directory. Otherwise, no guarantee is given.
   *
   * @throws IOException if locking fails
   */
  private void lock(File dir) throws IOException {
    FileLock lock = tryLock(dir);
    if (lock == null) {
      String msg = "Cannot lock " + dir
          + ". The directory is already locked.";
      LOGGER.info(msg);
      throw new IOException(msg);
    }
    FileLock secondLock = tryLock(dir);
    if(secondLock != null) {
      LOGGER.warn("Directory "+dir+" does not support locking");
      secondLock.release();
      secondLock.channel().close();
    }
    locks.put(dir.getAbsolutePath(), lock);
  }

  /**
   * Attempts to acquire an exclusive lock on the directory.
   *
   * @return A lock object representing the newly-acquired lock or
   * <code>null</code> if directory is already locked.
   * @throws IOException if locking fails.
   */
  private FileLock tryLock(File dir) throws IOException {
    File lockF = new File(dir, FILE_LOCK);
    lockF.deleteOnExit();
    RandomAccessFile file = new RandomAccessFile(lockF, "rws");
    FileLock res = null;
    try {
      res = file.getChannel().tryLock();
    } catch(OverlappingFileLockException oe) {
      file.close();
      return null;
    } catch(IOException e) {
      LOGGER.error("Cannot create lock on " + lockF, e);
      file.close();
      throw e;
    }
    return res;
  }

  /**
   * Unlock directory.
   *
   * @throws IOException
   */
  private void unlock(File dir) throws IOException {
    FileLock lock = locks.remove(dir.getAbsolutePath());
    if(lock == null) {
      return;
    }
    lock.release();
    lock.channel().close();
    lock = null;
  }
  static class BackgroundWorker extends Thread {
    private static final Logger LOG = LoggerFactory
        .getLogger(BackgroundWorker.class);
    private final Log log;
    private volatile boolean run = true;

    public BackgroundWorker(Log log) {
      this.log = log;
    }

    void shutdown() {
      if(run) {
        run = false;
        interrupt();
      }
    }

    @Override
    public void run() {
      while (run) {
        try {
          try {
            Thread.sleep(Math.max(1000L, log.checkpointInterval / 10L));
          } catch (InterruptedException e) {
            // recheck run flag
            continue;
          }
          if(log.open) {
            // check to see if we should do a checkpoint
            long elapsed = System.currentTimeMillis() - log.getLastCheckpoint();
            if (elapsed > log.checkpointInterval) {
              log.writeCheckpoint();
            }
          }
          if(log.open) {
            log.removeOldLogs();
          }
        } catch (IOException e) {
          LOG.error("Error doing checkpoint", e);
        } catch (Exception e) {
          LOG.error("General error in checkpoint worker", e);
        }
      }
    }
  }
}
