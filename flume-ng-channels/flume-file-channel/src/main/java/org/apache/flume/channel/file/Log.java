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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.SortedSet;

/**
 * Stores FlumeEvents on disk and pointers to the events in a in memory queue.
 * Once a log object is created the replay method should be called to reconcile
 * the on disk write ahead log with the last checkpoint of the queue.
 *
 * Before calling any of commitPut/commitTake/get/put/rollback/take
 * Log.tryLockShared should be called and the above operations
 * should only be called if tryLockShared returns true. After
 * the operation and any additional modifications of the
 * FlumeEventQueue, the Log.unlockShared method should be called.
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
  private final int queueCapacity;
  private final AtomicReferenceArray<LogFile.Writer> logFiles;

  private volatile boolean open;
  private FlumeEventQueue queue;
  private long checkpointInterval;
  private long maxFileSize;
  private final Map<String, FileLock> locks;
  private final ReentrantReadWriteLock checkpointLock =
      new ReentrantReadWriteLock(true);
  /**
   * Shared lock
   */
  private final ReadLock checkpointReadLock = checkpointLock.readLock();
  /**
   * Exclusive lock
   */
  private final WriteLock checkpointWriterLock = checkpointLock.writeLock();
  private int logWriteTimeout;
  private final String channelName;
  private final String channelNameDescriptor;
  private int checkpointWriteTimeout;
  private boolean useLogReplayV1;

  static class Builder {
    private long bCheckpointInterval;
    private long bMaxFileSize;
    private int bQueueCapacity;
    private File bCheckpointDir;
    private File[] bLogDirs;
    private int bLogWriteTimeout =
        FileChannelConfiguration.DEFAULT_WRITE_TIMEOUT;
    private String bName;
    private int bCheckpointWriteTimeout =
        FileChannelConfiguration.DEFAULT_CHECKPOINT_WRITE_TIMEOUT;
    private boolean useLogReplayV1;

    Builder setCheckpointInterval(long interval) {
      bCheckpointInterval = interval;
      return this;
    }

    Builder setMaxFileSize(long maxSize) {
      bMaxFileSize = maxSize;
      return this;
    }

    Builder setQueueSize(int capacity) {
      bQueueCapacity = capacity;
      return this;
    }

    Builder setCheckpointDir(File cpDir) {
      bCheckpointDir = cpDir;
      return this;
    }

    Builder setLogDirs(File[] dirs) {
      bLogDirs = dirs;
      return this;
    }

    Builder setLogWriteTimeout(int timeout) {
      bLogWriteTimeout = timeout;
      return this;
    }

    Builder setChannelName(String name) {
      bName = name;
      return this;
    }

    Builder setCheckpointWriteTimeout(int checkpointTimeout){
      bCheckpointWriteTimeout = checkpointTimeout;
      return this;
    }
    Builder setUseLogReplayV1(boolean useLogReplayV1){
      this.useLogReplayV1 = useLogReplayV1;
      return this;
    }

    Log build() throws IOException {
      return new Log(bCheckpointInterval, bMaxFileSize, bQueueCapacity,
          bLogWriteTimeout, bCheckpointWriteTimeout, bCheckpointDir, bName,
          useLogReplayV1, bLogDirs);
    }
  }

  private Log(long checkpointInterval, long maxFileSize, int queueCapacity,
      int logWriteTimeout, int checkpointWriteTimeout, File checkpointDir,
      String name, boolean useLogReplayV1, File... logDirs)
          throws IOException {
    Preconditions.checkArgument(checkpointInterval > 0,
        "checkpointInterval <= 0");
    Preconditions.checkArgument(queueCapacity > 0, "queueCapacity <= 0");
    Preconditions.checkArgument(maxFileSize > 0, "maxFileSize <= 0");
    Preconditions.checkNotNull(checkpointDir, "checkpointDir");
    Preconditions.checkArgument(
        checkpointDir.isDirectory() || checkpointDir.mkdirs(), "CheckpointDir "
            + checkpointDir + " could not be created");
    Preconditions.checkNotNull(logDirs, "logDirs");
    Preconditions.checkArgument(logDirs.length > 0, "logDirs empty");
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(),
            "channel name should be specified");

    this.channelName = name;
    this.channelNameDescriptor = "[channel=" + name + "]";
    this.useLogReplayV1 = useLogReplayV1;

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
    this.queueCapacity = queueCapacity;
    this.checkpointDir = checkpointDir;
    this.logDirs = logDirs;
    this.logWriteTimeout = logWriteTimeout;
    this.checkpointWriteTimeout = checkpointWriteTimeout;
    logFiles = new AtomicReferenceArray<LogFile.Writer>(this.logDirs.length);
    worker = new BackgroundWorker(this);
    worker.setName("Log-BackgroundWorker-" + name);
    worker.setDaemon(true);
    worker.start();
  }

  /**
   * Read checkpoint and data files from disk replaying them to the state
   * directly before the shutdown or crash.
   * @throws IOException
   */
  void replay() throws IOException {
    Preconditions.checkState(!open, "Cannot replay after Log has been opened");

    Preconditions.checkState(tryLockExclusive(), "Cannot obtain lock on "
        + channelNameDescriptor);

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
          ", from " + dataFiles);

      /*
       * sort the data files by file id so we can replay them by file id
       * which should approximately give us sequential events
       */
      LogUtils.sort(dataFiles);

      /*
       * Read the checkpoint (in memory queue) from one of two alternating
       * locations. We will read the last one written to disk.
       */
      File checkpointFile = new File(checkpointDir, "checkpoint");
      File inflightTakesFile = new File(checkpointDir, "inflighttakes");
      File inflightPutsFile = new File(checkpointDir, "inflightputs");
      queue = new FlumeEventQueue(queueCapacity, checkpointFile,
              inflightTakesFile, inflightPutsFile, channelName);
      LOGGER.info("Last Checkpoint " + new Date(checkpointFile.lastModified())
        + ", queue depth = " + queue.getSize());

      /*
       * We now have everything we need to actually replay the log files
       * the queue, the timestamp the queue was written to disk, and
       * the list of data files.
       */
      ReplayHandler replayHandler = new ReplayHandler(queue);
      if(useLogReplayV1) {
        LOGGER.info("Replaying logs with v1 replay logic");
        replayHandler.replayLogv1(dataFiles);
      } else {
        LOGGER.info("Replaying logs with v2 replay logic");
        replayHandler.replayLog(dataFiles);
      }


      for (int index = 0; index < logDirs.length; index++) {
        LOGGER.info("Rolling " + logDirs[index]);
        roll(index);
      }

      /*
       * Now that we have replayed, write the current queue to disk
       */
      writeCheckpoint(true);

      open = true;
    } catch (Exception ex) {
      LOGGER.error("Failed to initialize Log on " + channelNameDescriptor, ex);
      if (ex instanceof IOException) {
        throw (IOException) ex;
      }
      Throwables.propagate(ex);
    } finally {
      unlockExclusive();
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
    FlumeEvent flumeEvent = new FlumeEvent(
        event.getHeaders(), event.getBody());
    Put put = new Put(transactionID, flumeEvent);
    put.setLogWriteOrderID(WriteOrderOracle.next());
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
    take.setLogWriteOrderID(WriteOrderOracle.next());
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
    rollback.setLogWriteOrderID(WriteOrderOracle.next());
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


  private boolean tryLockExclusive() {
    try {
      return checkpointWriterLock.tryLock(checkpointWriteTimeout,
          TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOGGER.warn("Interrupted while waiting for log exclusive lock", ex);
      Thread.currentThread().interrupt();
    }
    return false;
  }
  private void unlockExclusive()  {
    checkpointWriterLock.unlock();
  }

  boolean tryLockShared() {
    try {
      return checkpointReadLock.tryLock(logWriteTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      LOGGER.warn("Interrupted while waiting for log shared lock", ex);
      Thread.currentThread().interrupt();
    }
    return false;
  }

  void unlockShared()  {
    checkpointReadLock.unlock();
  }

  private void lockExclusive(){
    checkpointWriterLock.lock();
  }

  /**
   * Synchronization required since we do not want this
   * to be called during a checkpoint.
   */
  synchronized void close() {
    lockExclusive();
    try {
      open = false;
      if (worker != null) {
        worker.shutdown();
        worker.interrupt();
      }
      if (logFiles != null) {
        for (int index = 0; index < logFiles.length(); index++) {
          logFiles.get(index).close();
        }
      }
      synchronized (idLogFileMap) {
        for (Integer logId : idLogFileMap.keySet()) {
          LogFile.RandomReader reader = idLogFileMap.get(logId);
          if (reader != null) {
            reader.close();
          }
        }
      }
      try {
        unlock(checkpointDir);
      } catch (IOException ex) {
        LOGGER.warn("Error unlocking " + checkpointDir, ex);
      }
      for (File logDir : logDirs) {
        try {
          unlock(logDir);
        } catch (IOException ex) {
          LOGGER.warn("Error unlocking " + logDir, ex);
        }
      }
    } finally {
      unlockExclusive();
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

    Preconditions.checkState(open, "Log is closed");
    Commit commit = new Commit(transactionID, type);
    commit.setLogWriteOrderID(WriteOrderOracle.next());
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
   * methods call this method, and this method acquires only a
   * read lock. The synchronization guarantees that multiple threads don't
   * roll at the same time.
   * @param index
   * @throws IOException
   */
  private synchronized void roll(int index, ByteBuffer buffer)
      throws IOException {
    if (!tryLockShared()) {
      throw new IOException("Failed to obtain lock for writing to the log. "
          + "Try increasing the log write timeout value or disabling it by "
          + "setting it to 0. "+ channelNameDescriptor);
    }

    try {
      LogFile.Writer oldLogFile = logFiles.get(index);
      // check to make sure a roll is actually required due to
      // the possibility of multiple writes waiting on lock
      if(oldLogFile == null || buffer == null ||
          oldLogFile.isRollRequired(buffer)) {
        try {
          LOGGER.info("Roll start " + logDirs[index]);
          int fileID = nextFileID.incrementAndGet();
          File file = new File(logDirs[index], PREFIX + fileID);
          Preconditions.checkState(!file.exists(),
              "File already exists "  + file);
          Preconditions.checkState(file.createNewFile(),
              "File could not be created " + file);
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
    } finally {
      unlockShared();
    }
  }

  private boolean writeCheckpoint() throws Exception {
    return writeCheckpoint(false);
  }

  /**
   * Write the current checkpoint object and then swap objects so that
   * the next checkpoint occurs on the other checkpoint directory.
   *
   * Synchronization is not required because this method acquires a
   * write lock. So this method gets exclusive access to all the
   * data structures this method accesses.
   * @param force  a flag to force the writing of checkpoint
   * @throws IOException if we are unable to write the checkpoint out to disk
   */
  private Boolean writeCheckpoint(Boolean force) throws Exception {
    boolean checkpointCompleted = false;
    boolean lockAcquired = tryLockExclusive();
    if(!lockAcquired) {
      return false;
    }
    SortedSet<Integer> idSet = null;
    try {
      if (queue.checkpoint(force) || force) {
        long logWriteOrderID = queue.getLogWriteOrderID();

        //Since the active files might also be in the queue's fileIDs,
        //we need to either move each one to a new set or remove each one
        //as we do here. Otherwise we cannot make sure every element in
        //fileID set from the queue have been updated.
        //Since clone is smarter than insert, better to make
        //a copy of the set first so that we can use it later.
        idSet = queue.getFileIDs();
        SortedSet<Integer> idSetToCompare = new TreeSet<Integer>(idSet);

        int numFiles = logFiles.length();
        for (int i = 0; i < numFiles; i++) {
          LogFile.Writer writer = logFiles.get(i);
          writer.markCheckpoint(logWriteOrderID);
          int id = writer.getFileID();
          idSet.remove(id);
          LOGGER.debug("Updated checkpoint for file: " + writer.getFile());
        }

        // Update any inactive data files as well
        Iterator<Integer> idIterator = idSet.iterator();
        while (idIterator.hasNext()) {
          int id = idIterator.next();
          LogFile.RandomReader reader = idLogFileMap.remove(id);
          File file = reader.getFile();
          reader.close();
          // Open writer in inactive mode
          LogFile.Writer writer =
              new LogFile.Writer(file, id, maxFileSize, false);
          writer.markCheckpoint(logWriteOrderID);
          writer.close();
          reader = new LogFile.RandomReader(file);
          idLogFileMap.put(id, reader);
          LOGGER.debug("Updated checkpoint for file: " + file);
          idIterator.remove();
        }
        Preconditions.checkState(idSet.size() == 0,
                "Could not update all data file timestamps: " + idSet);
        //Add files from all log directories
        for (int index = 0; index < logDirs.length; index++) {
          idSetToCompare.add(logFiles.get(index).getFileID());
        }
        idSet = idSetToCompare;
        checkpointCompleted = true;
      }
    } finally {
      unlockExclusive();
    }
    //Do the deletes outside the checkpointWriterLock
    //Delete logic is expensive.
    if (open && checkpointCompleted) {
      removeOldLogs(idSet);
    }
    //Since the exception is not caught, this will not be returned if
    //an exception is thrown from the try.
    return true;
  }

  private void removeOldLogs(SortedSet<Integer> fileIDs) {
    Preconditions.checkState(open, "Log is closed");
    // we will find the smallest fileID currently in use and
    // won't delete any files with an id larger than the min
    int minFileID = fileIDs.first();
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
          + ". The directory is already locked. "
          + channelNameDescriptor;
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
      }
    }

    @Override
    public void run() {
      long lastCheckTime = 0L;
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
            long currentTime = System.currentTimeMillis();
            long elapsed = currentTime - lastCheckTime;
            if (elapsed > log.checkpointInterval) {
              if(log.writeCheckpoint()) {
                lastCheckTime = currentTime;
              }
            }
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
