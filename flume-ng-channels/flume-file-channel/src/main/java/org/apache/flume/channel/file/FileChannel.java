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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.Log.Builder;
import org.apache.flume.channel.file.encryption.EncryptionConfiguration;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.apache.flume.channel.file.encryption.KeyProviderFactory;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A durable {@link Channel} implementation that uses the local file system for
 * its storage.
 * </p>
 * <p>
 * FileChannel works by writing all transactions to a set of directories
 * specified in the configuration. Additionally, when a commit occurs
 * the transaction is synced to disk.
 * </p>
 * <p>
 * FileChannel is marked
 * {@link org.apache.flume.annotations.InterfaceAudience.Private} because it
 * should only be instantiated via a configuration. For example, users should
 * certainly use FileChannel but not by instantiating FileChannel objects.
 * Meaning the label Private applies to user-developers not user-operators.
 * In cases where a Channel is required by instantiated by user-developers
 * {@link org.apache.flume.channel.MemoryChannel} should be used.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class FileChannel extends BasicChannelSemantics {

  private static final Logger LOG = LoggerFactory
      .getLogger(FileChannel.class);

  private Integer capacity = 0;
  private int keepAlive;
  protected Integer transactionCapacity = 0;
  private Long checkpointInterval = 0L;
  private long maxFileSize;
  private long minimumRequiredSpace;
  private File checkpointDir;
  private File backupCheckpointDir;
  private File[] dataDirs;
  private Log log;
  private volatile boolean open;
  private volatile Throwable startupError;
  private Semaphore queueRemaining;
  private final ThreadLocal<FileBackedTransaction> transactions =
      new ThreadLocal<FileBackedTransaction>();
  private String channelNameDescriptor = "[channel=unknown]";
  private ChannelCounter channelCounter;
  private boolean useLogReplayV1;
  private boolean useFastReplay = false;
  private KeyProvider encryptionKeyProvider;
  private String encryptionActiveKey;
  private String encryptionCipherProvider;
  private boolean useDualCheckpoints;
  private boolean compressBackupCheckpoint;
  private boolean fsyncPerTransaction;
  private int fsyncInterval;

  @Override
  public synchronized void setName(String name) {
    channelNameDescriptor = "[channel=" + name + "]";
    super.setName(name);
  }

  @Override
  public void configure(Context context) {

    useDualCheckpoints = context.getBoolean(
        FileChannelConfiguration.USE_DUAL_CHECKPOINTS,
        FileChannelConfiguration.DEFAULT_USE_DUAL_CHECKPOINTS);

    compressBackupCheckpoint = context.getBoolean(
        FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
        FileChannelConfiguration.DEFAULT_COMPRESS_BACKUP_CHECKPOINT);

    String homePath = System.getProperty("user.home").replace('\\', '/');

    String strCheckpointDir =
        context.getString(FileChannelConfiguration.CHECKPOINT_DIR,
            homePath + "/.flume/file-channel/checkpoint");

    String strBackupCheckpointDir = context.getString
      (FileChannelConfiguration.BACKUP_CHECKPOINT_DIR, "").trim();

    String[] strDataDirs = context.getString(FileChannelConfiguration.DATA_DIRS,
        homePath + "/.flume/file-channel/data").split(",");

    checkpointDir = new File(strCheckpointDir);

    if (useDualCheckpoints) {
      Preconditions.checkState(!strBackupCheckpointDir.isEmpty(),
        "Dual checkpointing is enabled, but the backup directory is not set. " +
          "Please set " + FileChannelConfiguration.BACKUP_CHECKPOINT_DIR + " " +
          "to enable dual checkpointing");
      backupCheckpointDir = new File(strBackupCheckpointDir);
      /*
       * If the backup directory is the same as the checkpoint directory,
       * then throw an exception and force the config system to ignore this
       * channel.
       */
      Preconditions.checkState(!backupCheckpointDir.equals(checkpointDir),
        "Could not configure " + getName() + ". The checkpoint backup " +
          "directory and the checkpoint directory are " +
          "configured to be the same.");
    }

    dataDirs = new File[strDataDirs.length];
    for (int i = 0; i < strDataDirs.length; i++) {
      dataDirs[i] = new File(strDataDirs[i]);
    }

    capacity = context.getInteger(FileChannelConfiguration.CAPACITY,
        FileChannelConfiguration.DEFAULT_CAPACITY);
    if(capacity <= 0) {
      capacity = FileChannelConfiguration.DEFAULT_CAPACITY;
      LOG.warn("Invalid capacity specified, initializing channel to "
              + "default capacity of {}", capacity);
    }

    keepAlive =
        context.getInteger(FileChannelConfiguration.KEEP_ALIVE,
            FileChannelConfiguration.DEFAULT_KEEP_ALIVE);
    transactionCapacity =
        context.getInteger(FileChannelConfiguration.TRANSACTION_CAPACITY,
            FileChannelConfiguration.DEFAULT_TRANSACTION_CAPACITY);

    if(transactionCapacity <= 0) {
      transactionCapacity =
              FileChannelConfiguration.DEFAULT_TRANSACTION_CAPACITY;
      LOG.warn("Invalid transaction capacity specified, " +
          "initializing channel to default " +
          "capacity of {}", transactionCapacity);
    }

    Preconditions.checkState(transactionCapacity <= capacity,
      "File Channel transaction capacity cannot be greater than the " +
        "capacity of the channel.");

    checkpointInterval =
            context.getLong(FileChannelConfiguration.CHECKPOINT_INTERVAL,
            FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL);
    if (checkpointInterval <= 0) {
      LOG.warn("Checkpoint interval is invalid: " + checkpointInterval
              + ", using default: "
              + FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL);

      checkpointInterval =
              FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL;
    }

    // cannot be over FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE
    maxFileSize = Math.min(
      context.getLong(FileChannelConfiguration.MAX_FILE_SIZE,
        FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE),
      FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE);

    minimumRequiredSpace = Math.max(
      context.getLong(FileChannelConfiguration.MINIMUM_REQUIRED_SPACE,
        FileChannelConfiguration.DEFAULT_MINIMUM_REQUIRED_SPACE),
      FileChannelConfiguration.FLOOR_MINIMUM_REQUIRED_SPACE);

    useLogReplayV1 = context.getBoolean(
        FileChannelConfiguration.USE_LOG_REPLAY_V1,
          FileChannelConfiguration.DEFAULT_USE_LOG_REPLAY_V1);

    useFastReplay = context.getBoolean(
            FileChannelConfiguration.USE_FAST_REPLAY,
            FileChannelConfiguration.DEFAULT_USE_FAST_REPLAY);

    Context encryptionContext = new Context(
        context.getSubProperties(EncryptionConfiguration.ENCRYPTION_PREFIX +
            "."));
    String encryptionKeyProviderName = encryptionContext.getString(
        EncryptionConfiguration.KEY_PROVIDER);
    encryptionActiveKey = encryptionContext.getString(
        EncryptionConfiguration.ACTIVE_KEY);
    encryptionCipherProvider = encryptionContext.getString(
        EncryptionConfiguration.CIPHER_PROVIDER);
    if(encryptionKeyProviderName != null) {
      Preconditions.checkState(!Strings.isNullOrEmpty(encryptionActiveKey),
          "Encryption configuration problem: " +
              EncryptionConfiguration.ACTIVE_KEY + " is missing");
      Preconditions.checkState(!Strings.isNullOrEmpty(encryptionCipherProvider),
          "Encryption configuration problem: " +
              EncryptionConfiguration.CIPHER_PROVIDER + " is missing");
      Context keyProviderContext = new Context(encryptionContext.
          getSubProperties(EncryptionConfiguration.KEY_PROVIDER + "."));
      encryptionKeyProvider = KeyProviderFactory.
          getInstance(encryptionKeyProviderName, keyProviderContext);
    } else {
      Preconditions.checkState(encryptionActiveKey == null,
          "Encryption configuration problem: " +
              EncryptionConfiguration.ACTIVE_KEY + " is present while key " +
          "provider name is not.");
      Preconditions.checkState(encryptionCipherProvider == null,
          "Encryption configuration problem: " +
              EncryptionConfiguration.CIPHER_PROVIDER + " is present while " +
          "key provider name is not.");
    }

    fsyncPerTransaction = context.getBoolean(FileChannelConfiguration
      .FSYNC_PER_TXN, FileChannelConfiguration.DEFAULT_FSYNC_PRE_TXN);

    fsyncInterval = context.getInteger(FileChannelConfiguration
      .FSYNC_INTERVAL, FileChannelConfiguration.DEFAULT_FSYNC_INTERVAL);

    if(queueRemaining == null) {
      queueRemaining = new Semaphore(capacity, true);
    }
    if(log != null) {
      log.setCheckpointInterval(checkpointInterval);
      log.setMaxFileSize(maxFileSize);
    }

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting {}...", this);
    try {
      Builder builder = new Log.Builder();
      builder.setCheckpointInterval(checkpointInterval);
      builder.setMaxFileSize(maxFileSize);
      builder.setMinimumRequiredSpace(minimumRequiredSpace);
      builder.setQueueSize(capacity);
      builder.setCheckpointDir(checkpointDir);
      builder.setLogDirs(dataDirs);
      builder.setChannelName(getName());
      builder.setUseLogReplayV1(useLogReplayV1);
      builder.setUseFastReplay(useFastReplay);
      builder.setEncryptionKeyProvider(encryptionKeyProvider);
      builder.setEncryptionKeyAlias(encryptionActiveKey);
      builder.setEncryptionCipherProvider(encryptionCipherProvider);
      builder.setUseDualCheckpoints(useDualCheckpoints);
      builder.setCompressBackupCheckpoint(compressBackupCheckpoint);
      builder.setBackupCheckpointDir(backupCheckpointDir);
      builder.setFsyncPerTransaction(fsyncPerTransaction);
      builder.setFsyncInterval(fsyncInterval);
      log = builder.build();
      log.replay();
      open = true;

      int depth = getDepth();
      Preconditions.checkState(queueRemaining.tryAcquire(depth),
          "Unable to acquire " + depth + " permits " + channelNameDescriptor);
      LOG.info("Queue Size after replay: " + depth + " "
           + channelNameDescriptor);
    } catch (Throwable t) {
      open = false;
      startupError = t;
      LOG.error("Failed to start the file channel " + channelNameDescriptor, t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
    if (open) {
      channelCounter.start();
      channelCounter.setChannelSize(getDepth());
      channelCounter.setChannelCapacity(capacity);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}...", this);
    startupError = null;
    int size = getDepth();
    close();
    if (!open) {
      channelCounter.setChannelSize(size);
      channelCounter.stop();
    }
    super.stop();
  }

  @Override
  public String toString() {
    return "FileChannel " + getName() + " { dataDirs: " +
        Arrays.toString(dataDirs) + " }";
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    if(!open) {
      String msg = "Channel closed " + channelNameDescriptor;
      if(startupError != null) {
        msg += ". Due to " + startupError.getClass().getName() + ": " +
            startupError.getMessage();
        throw new IllegalStateException(msg, startupError);
      }
      throw new IllegalStateException(msg);
    }

    FileBackedTransaction trans = transactions.get();
    if(trans != null && !trans.isClosed()) {
      Preconditions.checkState(false,
          "Thread has transaction which is still open: " +
              trans.getStateAsString()  + channelNameDescriptor);
    }
    trans = new FileBackedTransaction(log, TransactionIDOracle.next(),
      transactionCapacity, keepAlive, queueRemaining, getName(),
      fsyncPerTransaction, channelCounter);
    transactions.set(trans);
    return trans;
  }

  protected int getDepth() {
    Preconditions.checkState(open, "Channel closed"  + channelNameDescriptor);
    Preconditions.checkNotNull(log, "log");
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Preconditions.checkNotNull(queue, "queue");
    return queue.getSize();
  }
  void close() {
    if(open) {
      open = false;
      try {
        log.close();
      } catch (Exception e) {
        LOG.error("Error while trying to close the log.", e);
        Throwables.propagate(e);
      }
      log = null;
      queueRemaining = null;
    }
  }

  @VisibleForTesting
  boolean didFastReplay() {
    return log.didFastReplay();
  }


  @VisibleForTesting
  boolean didFullReplayDueToBadCheckpointException() {
    return log.didFullReplayDueToBadCheckpointException();
  }

  public boolean isOpen() {
    return open;
  }

  /**
   * Did this channel recover a backup of the checkpoint to restart?
   * @return true if the channel recovered using a backup.
   */
  @VisibleForTesting
  boolean checkpointBackupRestored() {
    if(log != null) {
      return log.backupRestored();
    }
    return false;
  }

  @VisibleForTesting
  Log getLog() {
    return log;
  }

  /**
   * Transaction backed by a file. This transaction supports either puts
   * or takes but not both.
   */
  static class FileBackedTransaction extends BasicTransactionSemantics {
    private final LinkedBlockingDeque<FlumeEventPointer> takeList;
    private final LinkedBlockingDeque<FlumeEventPointer> putList;
    private final long transactionID;
    private final int keepAlive;
    private final Log log;
    private final FlumeEventQueue queue;
    private final Semaphore queueRemaining;
    private final String channelNameDescriptor;
    private final ChannelCounter channelCounter;
    private final boolean fsyncPerTransaction;
    public FileBackedTransaction(Log log, long transactionID,
        int transCapacity, int keepAlive, Semaphore queueRemaining,
        String name, boolean fsyncPerTransaction, ChannelCounter
      counter) {
      this.log = log;
      queue = log.getFlumeEventQueue();
      this.transactionID = transactionID;
      this.keepAlive = keepAlive;
      this.queueRemaining = queueRemaining;
      putList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      takeList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      this.fsyncPerTransaction = fsyncPerTransaction;
      channelNameDescriptor = "[channel=" + name + "]";
      this.channelCounter = counter;
    }
    private boolean isClosed() {
      return State.CLOSED.equals(getState());
    }
    private String getStateAsString() {
      return String.valueOf(getState());
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();
      if(putList.remainingCapacity() == 0) {
        throw new ChannelException("Put queue for FileBackedTransaction " +
            "of capacity " + putList.size() + " full, consider " +
            "committing more frequently, increasing capacity or " +
            "increasing thread count. " + channelNameDescriptor);
      }
      // this does not need to be in the critical section as it does not
      // modify the structure of the log or queue.
      if(!queueRemaining.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        throw new ChannelFullException("The channel has reached it's capacity. "
            + "This might be the result of a sink on the channel having too "
            + "low of batch size, a downstream system running slower than "
            + "normal, or that the channel capacity is just too low. "
            + channelNameDescriptor);
      }
      boolean success = false;
      log.lockShared();
      try {
        FlumeEventPointer ptr = log.put(transactionID, event);
        Preconditions.checkState(putList.offer(ptr), "putList offer failed "
          + channelNameDescriptor);
        queue.addWithoutCommit(ptr, transactionID);
        success = true;
      } catch (IOException e) {
        throw new ChannelException("Put failed due to IO error "
                + channelNameDescriptor, e);
      } finally {
        log.unlockShared();
        if(!success) {
          // release slot obtained in the case
          // the put fails for any reason
          queueRemaining.release();
        }
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      if(takeList.remainingCapacity() == 0) {
        throw new ChannelException("Take list for FileBackedTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count. "
               + channelNameDescriptor);
      }
      log.lockShared();
      /*
       * 1. Take an event which is in the queue.
       * 2. If getting that event does not throw NoopRecordException,
       *    then return it.
       * 3. Else try to retrieve the next event from the queue
       * 4. Repeat 2 and 3 until queue is empty or an event is returned.
       */

      try {
        while (true) {
          FlumeEventPointer ptr = queue.removeHead(transactionID);
          if (ptr == null) {
            return null;
          } else {
            try {
              // first add to takeList so that if write to disk
              // fails rollback actually does it's work
              Preconditions.checkState(takeList.offer(ptr),
                "takeList offer failed "
                  + channelNameDescriptor);
              log.take(transactionID, ptr); // write take to disk
              Event event = log.get(ptr);
              return event;
            } catch (IOException e) {
              throw new ChannelException("Take failed due to IO error "
                + channelNameDescriptor, e);
            } catch (NoopRecordException e) {
              LOG.warn("Corrupt record replaced by File Channel Integrity " +
                "tool found. Will retrieve next event", e);
              takeList.remove(ptr);
            } catch (CorruptEventException ex) {
              if (fsyncPerTransaction) {
                throw new ChannelException(ex);
              }
              LOG.warn("Corrupt record found. Event will be " +
                "skipped, and next event will be read.", ex);
              takeList.remove(ptr);
            }
          }
        }
      } finally {
        log.unlockShared();
      }
    }
    @Override
    protected void doCommit() throws InterruptedException {
      int puts = putList.size();
      int takes = takeList.size();
      if(puts > 0) {
        Preconditions.checkState(takes == 0, "nonzero puts and takes "
                + channelNameDescriptor);
        log.lockShared();
        try {
          log.commitPut(transactionID);
          channelCounter.addToEventPutSuccessCount(puts);
          synchronized (queue) {
            while(!putList.isEmpty()) {
              if(!queue.addTail(putList.removeFirst())) {
                StringBuilder msg = new StringBuilder();
                msg.append("Queue add failed, this shouldn't be able to ");
                msg.append("happen. A portion of the transaction has been ");
                msg.append("added to the queue but the remaining portion ");
                msg.append("cannot be added. Those messages will be consumed ");
                msg.append("despite this transaction failing. Please report.");
                msg.append(channelNameDescriptor);
                LOG.error(msg.toString());
                Preconditions.checkState(false, msg.toString());
              }
            }
            queue.completeTransaction(transactionID);
          }
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
                  + channelNameDescriptor, e);
        } finally {
          log.unlockShared();
        }

      } else if (takes > 0) {
        log.lockShared();
        try {
          log.commitTake(transactionID);
          queue.completeTransaction(transactionID);
          channelCounter.addToEventTakeSuccessCount(takes);
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
              + channelNameDescriptor, e);
        } finally {
          log.unlockShared();
        }
        queueRemaining.release(takes);
      }
      putList.clear();
      takeList.clear();
      channelCounter.setChannelSize(queue.getSize());
    }
    @Override
    protected void doRollback() throws InterruptedException {
      int puts = putList.size();
      int takes = takeList.size();
      log.lockShared();
      try {
        if(takes > 0) {
          Preconditions.checkState(puts == 0, "nonzero puts and takes "
              + channelNameDescriptor);
          synchronized (queue) {
            while (!takeList.isEmpty()) {
              Preconditions.checkState(queue.addHead(takeList.removeLast()),
                  "Queue add failed, this shouldn't be able to happen "
                      + channelNameDescriptor);
            }
          }
        }
        putList.clear();
        takeList.clear();
        queue.completeTransaction(transactionID);
        channelCounter.setChannelSize(queue.getSize());
        log.rollback(transactionID);
      } catch (IOException e) {
        throw new ChannelException("Commit failed due to IO error "
            + channelNameDescriptor, e);
      } finally {
        log.unlockShared();
        // since rollback is being called, puts will never make it on
        // to the queue and we need to be sure to release the resources
        queueRemaining.release(puts);
      }
    }
  }
}
