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
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
@InterfaceAudience.Private
@InterfaceStability.Unstable
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
  private final int queueCapacity;
  private final AtomicReferenceArray<LogFile.Writer> logFiles;

  private final ScheduledExecutorService workerExecutor;

  private volatile boolean open;
  private FlumeEventQueue queue;
  private long checkpointInterval;
  private long maxFileSize;
  private final boolean useFastReplay;
  private final long minimumRequiredSpace;
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
  private final String channelNameDescriptor;
  private int checkpointWriteTimeout;
  private boolean useLogReplayV1;
  private KeyProvider encryptionKeyProvider;
  private String encryptionCipherProvider;
  private String encryptionKeyAlias;
  private Key encryptionKey;
  private final long usableSpaceRefreshInterval;

  static class Builder {
    private long bCheckpointInterval;
    private long bMinimumRequiredSpace;
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
    private boolean useFastReplay;
    private KeyProvider bEncryptionKeyProvider;
    private String bEncryptionKeyAlias;
    private String bEncryptionCipherProvider;
    private long bUsableSpaceRefreshInterval = 15L * 1000L;

    Builder setUsableSpaceRefreshInterval(long usableSpaceRefreshInterval) {
      bUsableSpaceRefreshInterval = usableSpaceRefreshInterval;
      return this;
    }

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

    Builder setMinimumRequiredSpace(long minimumRequiredSpace) {
      bMinimumRequiredSpace = minimumRequiredSpace;
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

    Builder setUseFastReplay(boolean useFastReplay){
      this.useFastReplay = useFastReplay;
      return this;
    }

    Builder setEncryptionKeyProvider(KeyProvider encryptionKeyProvider) {
      bEncryptionKeyProvider = encryptionKeyProvider;
      return this;
    }

    Builder setEncryptionKeyAlias(String encryptionKeyAlias) {
      bEncryptionKeyAlias = encryptionKeyAlias;
      return this;
    }

    Builder setEncryptionCipherProvider(String encryptionCipherProvider) {
      bEncryptionCipherProvider = encryptionCipherProvider;
      return this;
    }

    Log build() throws IOException {
      return new Log(bCheckpointInterval, bMaxFileSize, bQueueCapacity,
          bLogWriteTimeout, bCheckpointWriteTimeout, bCheckpointDir, bName,
          useLogReplayV1, useFastReplay, bMinimumRequiredSpace,
          bEncryptionKeyProvider, bEncryptionKeyAlias,
          bEncryptionCipherProvider, bUsableSpaceRefreshInterval,
          bLogDirs);
    }
  }

  private Log(long checkpointInterval, long maxFileSize, int queueCapacity,
      int logWriteTimeout, int checkpointWriteTimeout, File checkpointDir,
      String name, boolean useLogReplayV1, boolean useFastReplay,
      long minimumRequiredSpace, @Nullable KeyProvider encryptionKeyProvider,
      @Nullable String encryptionKeyAlias,
      @Nullable String encryptionCipherProvider,
      long usableSpaceRefreshInterval, File... logDirs)
          throws IOException {
    Preconditions.checkArgument(checkpointInterval > 0,
        "checkpointInterval <= 0");
    Preconditions.checkArgument(queueCapacity > 0, "queueCapacity <= 0");
    Preconditions.checkArgument(maxFileSize > 0, "maxFileSize <= 0");
    Preconditions.checkNotNull(checkpointDir, "checkpointDir");
    Preconditions.checkArgument(usableSpaceRefreshInterval > 0,
        "usableSpaceRefreshInterval <= 0");
    Preconditions.checkArgument(
        checkpointDir.isDirectory() || checkpointDir.mkdirs(), "CheckpointDir "
            + checkpointDir + " could not be created");
    Preconditions.checkNotNull(logDirs, "logDirs");
    Preconditions.checkArgument(logDirs.length > 0, "logDirs empty");
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(),
            "channel name should be specified");

    this.channelNameDescriptor = "[channel=" + name + "]";
    this.useLogReplayV1 = useLogReplayV1;
    this.useFastReplay = useFastReplay;
    this.minimumRequiredSpace = minimumRequiredSpace;
    this.usableSpaceRefreshInterval = usableSpaceRefreshInterval;
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
    if(encryptionKeyProvider != null && encryptionKeyAlias != null &&
        encryptionCipherProvider != null) {
      LOGGER.info("Encryption is enabled with encryptionKeyProvider = " +
          encryptionKeyProvider + ", encryptionKeyAlias = " + encryptionKeyAlias
          + ", encryptionCipherProvider = " + encryptionCipherProvider);
      this.encryptionKeyProvider = encryptionKeyProvider;
      this.encryptionKeyAlias = encryptionKeyAlias;
      this.encryptionCipherProvider = encryptionCipherProvider;
      this.encryptionKey = encryptionKeyProvider.getKey(encryptionKeyAlias);
    } else if (encryptionKeyProvider == null && encryptionKeyAlias == null &&
        encryptionCipherProvider == null) {
      LOGGER.info("Encryption is not enabled");
    } else {
      throw new IllegalArgumentException("Encryption configuration must all " +
          "null or all not null: encryptionKeyProvider = " +
          encryptionKeyProvider + ", encryptionKeyAlias = " +
          encryptionKeyAlias +  ", encryptionCipherProvider = " +
          encryptionCipherProvider);
    }
    open = false;
    this.checkpointInterval = Math.max(checkpointInterval, 1000);
    this.maxFileSize = maxFileSize;
    this.queueCapacity = queueCapacity;
    this.checkpointDir = checkpointDir;
    this.logDirs = logDirs;
    this.logWriteTimeout = logWriteTimeout;
    this.checkpointWriteTimeout = checkpointWriteTimeout;
    logFiles = new AtomicReferenceArray<LogFile.Writer>(this.logDirs.length);
    workerExecutor = Executors.newSingleThreadScheduledExecutor(new
        ThreadFactoryBuilder().setNameFormat("Log-BackgroundWorker-" + name)
        .build());
    workerExecutor.scheduleWithFixedDelay(new BackgroundWorker(this),
        this.checkpointInterval, this.checkpointInterval,
        TimeUnit.MILLISECONDS);
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
          idLogFileMap.put(id, LogFileFactory.getRandomReader(new File(logDir,
              PREFIX + id), encryptionKeyProvider));
        }
      }
      LOGGER.info("Found NextFileID " + nextFileID +
          ", from " + dataFiles);

      /*
       * sort the data files by file id so we can replay them by file id
       * which should approximately give us sequential events
       */
      LogUtils.sort(dataFiles);

      boolean useFastReplay = this.useFastReplay;
      /*
       * Read the checkpoint (in memory queue) from one of two alternating
       * locations. We will read the last one written to disk.
       */
      File checkpointFile = new File(checkpointDir, "checkpoint");
      if(useFastReplay) {
        if(checkpointFile.exists()) {
          LOGGER.debug("Disabling fast full replay because checkpoint " +
              "exists: " + checkpointFile);
          useFastReplay = false;
        } else {
          LOGGER.debug("Not disabling fast full replay because checkpoint " +
              " does not exist: " + checkpointFile);
        }
      }
      File inflightTakesFile = new File(checkpointDir, "inflighttakes");
      File inflightPutsFile = new File(checkpointDir, "inflightputs");
      EventQueueBackingStore backingStore = null;


      try {
        backingStore =
                EventQueueBackingStoreFactory.get(checkpointFile, queueCapacity,
                channelNameDescriptor);
        queue = new FlumeEventQueue(backingStore, inflightTakesFile,
                inflightPutsFile);
        LOGGER.info("Last Checkpoint " + new Date(checkpointFile.lastModified())
                + ", queue depth = " + queue.getSize());

        /*
         * We now have everything we need to actually replay the log files
         * the queue, the timestamp the queue was written to disk, and
         * the list of data files.
         *
         * This will throw if and only if checkpoint file was fine,
         * but the inflights were not. If the checkpoint was bad, the backing
         * store factory would have thrown.
         */
        doReplay(queue, dataFiles, encryptionKeyProvider);
      } catch (BadCheckpointException ex) {
        LOGGER.warn("Checkpoint may not have completed successfully. "
                + "Forcing full replay, this may take a while.", ex);
        if(!Serialization.deleteAllFiles(checkpointDir)) {
          throw new IOException("Could not delete files in checkpoint " +
              "directory to recover from a corrupt or incomplete checkpoint");
        }
        backingStore = EventQueueBackingStoreFactory.get(checkpointFile,
                queueCapacity, channelNameDescriptor);
        queue = new FlumeEventQueue(backingStore, inflightTakesFile,
                inflightPutsFile);
        doReplay(queue, dataFiles, encryptionKeyProvider);
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

  @SuppressWarnings("deprecation")
  private void doReplay(FlumeEventQueue queue, List<File> dataFiles,
          KeyProvider encryptionKeyProvider) throws Exception {
    CheckpointRebuilder rebuilder = new CheckpointRebuilder(dataFiles,
            queue);
    if (useFastReplay && rebuilder.rebuild()) {
      LOGGER.info("Fast replay successful.");
    } else {
      ReplayHandler replayHandler = new ReplayHandler(queue,
              encryptionKeyProvider);
      if (useLogReplayV1) {
        LOGGER.info("Replaying logs with v1 replay logic");
        replayHandler.replayLogv1(dataFiles);
      } else {
        LOGGER.info("Replaying logs with v2 replay logic");
        replayHandler.replayLog(dataFiles);
      }
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
    Put put = new Put(transactionID, WriteOrderOracle.next(), flumeEvent);
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(put);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        FlumeEventPointer ptr = logFiles.get(logFileIndex).put(buffer);
        error = false;
        return ptr;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        FlumeEventPointer ptr = logFiles.get(logFileIndex).put(buffer);
        error = false;
        return ptr;
      }
    } finally {
      if(error && open) {
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
    Take take = new Take(transactionID, WriteOrderOracle.next(),
        pointer.getOffset(), pointer.getFileID());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(take);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        logFiles.get(logFileIndex).take(buffer);
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        logFiles.get(logFileIndex).take(buffer);
        error = false;
      }
    } finally {
      if(error && open) {
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
    Rollback rollback = new Rollback(transactionID, WriteOrderOracle.next());
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(rollback);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        logFiles.get(logFileIndex).rollback(buffer);
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        logFiles.get(logFileIndex).rollback(buffer);
        error = false;
      }
    } finally {
      if(error && open) {
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
   * Synchronization not required since this method gets the write lock,
   * so checkpoint and this method cannot run at the same time.
   */
  void close() {
    lockExclusive();
    try {
      open = false;
      shutdownWorker();
      if (logFiles != null) {
        for (int index = 0; index < logFiles.length(); index++) {
          LogFile.Writer writer = logFiles.get(index);
          if(writer != null) {
            writer.close();
          }
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
      queue.close();
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

  void shutdownWorker() {
    String msg = "Attempting to shutdown background worker.";
    System.out.println(msg);
    LOGGER.info(msg);
    workerExecutor.shutdown();
    try {
      workerExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for worker to die.");
    }
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
    Commit commit = new Commit(transactionID, WriteOrderOracle.next(), type);
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(commit);
    int logFileIndex = nextLogWriter(transactionID);
    long usableSpace = logFiles.get(logFileIndex).getUsableSpace();
    long requiredSpace = minimumRequiredSpace + buffer.limit();
    if(usableSpace <= requiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + requiredSpace + " bytes");
    }
    boolean error = true;
    try {
      try {
        logFiles.get(logFileIndex).commit(buffer);
        error = false;
      } catch (LogFileRetryableIOException e) {
        if(!open) {
          throw e;
        }
        roll(logFileIndex, buffer);
        logFiles.get(logFileIndex).commit(buffer);
        error = false;
      }
    } finally {
      if(error && open) {
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
   *
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
   *
   * @param index
   * @throws IOException
   */
    private synchronized void roll(int index, ByteBuffer buffer)
      throws IOException {
    if (!tryLockShared()) {
      throw new ChannelException("Failed to obtain lock for writing to the "
          + "log. Try increasing the log write timeout value. " +
          channelNameDescriptor);
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
          LogFile.Writer writer = LogFileFactory.getWriter(file, fileID,
              maxFileSize, encryptionKey, encryptionKeyAlias,
              encryptionCipherProvider, usableSpaceRefreshInterval);
          idLogFileMap.put(fileID, LogFileFactory.getRandomReader(file,
              encryptionKeyProvider));
          // writer from this point on will get new reference
          logFiles.set(index, writer);
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
    long usableSpace = checkpointDir.getUsableSpace();
    if(usableSpace <= minimumRequiredSpace) {
      throw new IOException("Usable space exhaused, only " + usableSpace +
          " bytes remaining, required " + minimumRequiredSpace + " bytes");
    }
    boolean lockAcquired = tryLockExclusive();
    if(!lockAcquired) {
      return false;
    }
    SortedSet<Integer> logFileRefCountsAll = null, logFileRefCountsActive = null;
    try {
      if (queue.checkpoint(force)) {
        long logWriteOrderID = queue.getLogWriteOrderID();

        //Since the active files might also be in the queue's fileIDs,
        //we need to either move each one to a new set or remove each one
        //as we do here. Otherwise we cannot make sure every element in
        //fileID set from the queue have been updated.
        //Since clone is smarter than insert, better to make
        //a copy of the set first so that we can use it later.
        logFileRefCountsAll = queue.getFileIDs();
        logFileRefCountsActive = new TreeSet<Integer>(logFileRefCountsAll);

        int numFiles = logFiles.length();
        for (int i = 0; i < numFiles; i++) {
          LogFile.Writer logWriter = logFiles.get(i);
          int logFileID = logWriter.getLogFileID();
          File logFile = logWriter.getFile();
          LogFile.MetaDataWriter writer =
              LogFileFactory.getMetaDataWriter(logFile, logFileID);
          try {
            writer.markCheckpoint(logWriter.position(), logWriteOrderID);
          } finally {
            writer.close();
          }
          logFileRefCountsAll.remove(logFileID);
          LOGGER.info("Updated checkpoint for file: " + logFile + " position: "
              + logWriter.position() + " logWriteOrderID: " + logWriteOrderID);
        }

        // Update any inactive data files as well
        Iterator<Integer> idIterator = logFileRefCountsAll.iterator();
        while (idIterator.hasNext()) {
          int id = idIterator.next();
          LogFile.RandomReader reader = idLogFileMap.remove(id);
          File file = reader.getFile();
          reader.close();
          LogFile.MetaDataWriter writer =
              LogFileFactory.getMetaDataWriter(file, id);
          try {
            writer.markCheckpoint(logWriteOrderID);
          } finally {
            writer.close();
          }
          reader = LogFileFactory.getRandomReader(file, encryptionKeyProvider);
          idLogFileMap.put(id, reader);
          LOGGER.debug("Updated checkpoint for file: " + file
              + "logWriteOrderID " + logWriteOrderID);
          idIterator.remove();
        }
        Preconditions.checkState(logFileRefCountsAll.size() == 0,
                "Could not update all data file timestamps: " + logFileRefCountsAll);
        //Add files from all log directories
        for (int index = 0; index < logDirs.length; index++) {
          logFileRefCountsActive.add(logFiles.get(index).getLogFileID());
        }
        checkpointCompleted = true;
      }
    } finally {
      unlockExclusive();
    }
    //Do the deletes outside the checkpointWriterLock
    //Delete logic is expensive.
    if (open && checkpointCompleted) {
      removeOldLogs(logFileRefCountsActive);
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
          File metaDataFile = Serialization.getMetaDataFile(logFile);
          if(metaDataFile.exists() && !metaDataFile.delete()) {
            LOGGER.warn("Could not remove metadata file "
                + metaDataFile + " for " + logFile);
          }
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
  @SuppressWarnings("resource")
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
  static class BackgroundWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
        .getLogger(BackgroundWorker.class);
    private final Log log;

    public BackgroundWorker(Log log) {
      this.log = log;
    }

    @Override
    public void run() {
      try {
        if (log.open) {
          log.writeCheckpoint();
        }
      } catch (IOException e) {
        LOG.error("Error doing checkpoint", e);
      } catch (Throwable e) {
        LOG.error("General error in checkpoint worker", e);
      }
    }
  }
}
