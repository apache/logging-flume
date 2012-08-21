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
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.Log.Builder;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A durable {@link Channel} implementation that uses the local file system for
 * its storage.
 * </p>
 * <p>
 * FileChannel works by writing all transactions to a set of directories
 * specified in the configuration. Additionally, when a commit occurs
 * the transaction is synced to disk. Pointers to events put on the
 * channel are stored in memory. As such, each event on the queue
 * will require 8 bytes of DirectMemory (non-heap). However, the channel
 * will only allow a configurable number messages into the channel.
 * The appropriate amount of direct memory for said capacity,
 * must be allocated to the JVM via the JVM property: -XX:MaxDirectMemorySize
 * </p>
 * <br>
 * <p>
 * Memory Consumption:
 * <ol>
 * <li>200GB of data in queue at 100 byte messages: 16GB</li>
 * <li>200GB of data in queue at 500 byte messages: 3.2GB</li>
 * <li>200GB of data in queue at 1000 byte messages: 1.6GB</li>
 * </ol>
 * </p>
 */
public class FileChannel extends BasicChannelSemantics {

  private static final Logger LOG = LoggerFactory
      .getLogger(FileChannel.class);

  private int capacity;
  private int keepAlive;
  private int transactionCapacity;
  private long checkpointInterval;
  private long maxFileSize;
  private File checkpointDir;
  private File[] dataDirs;
  private Log log;
  private volatile boolean open;
  private Semaphore queueRemaining;
  private final ThreadLocal<FileBackedTransaction> transactions =
      new ThreadLocal<FileBackedTransaction>();
  private int logWriteTimeout;
  private int checkpointWriteTimeout;
  private String channelNameDescriptor = "[channel=unknown]";
  private ChannelCounter channelCounter;
  private boolean useLogReplayV1;

  @Override
  public synchronized void setName(String name) {
    channelNameDescriptor = "[channel=" + name + "]";
    super.setName(name);
  }

  @Override
  public void configure(Context context) {

    String homePath = System.getProperty("user.home").replace('\\', '/');

    String strCheckpointDir =
        context.getString(FileChannelConfiguration.CHECKPOINT_DIR,
            homePath + "/.flume/file-channel/checkpoint");

    String[] strDataDirs = context.getString(FileChannelConfiguration.DATA_DIRS,
        homePath + "/.flume/file-channel/data").split(",");

    if(checkpointDir == null) {
      checkpointDir = new File(strCheckpointDir);
    } else if(!checkpointDir.getAbsolutePath().
        equals(new File(strCheckpointDir).getAbsolutePath())) {
      LOG.warn("An attempt was made to change the checkpoint " +
          "directory after start, this is not supported.");
    }
    if(dataDirs == null) {
      dataDirs = new File[strDataDirs.length];
      for (int i = 0; i < strDataDirs.length; i++) {
        dataDirs[i] = new File(strDataDirs[i]);
      }
    } else {
      boolean changed = false;
      if(dataDirs.length != strDataDirs.length) {
        changed = true;
      } else {
        for (int i = 0; i < strDataDirs.length; i++) {
          if(!dataDirs[i].getAbsolutePath().
              equals(new File(strDataDirs[i]).getAbsolutePath())) {
            changed = true;
            break;
          }
        }
      }
      if(changed) {
        LOG.warn("An attempt was made to change the data " +
            "directories after start, this is not supported.");
      }
    }

    int newCapacity = context.getInteger(FileChannelConfiguration.CAPACITY,
        FileChannelConfiguration.DEFAULT_CAPACITY);
    if(capacity > 0 && newCapacity != capacity) {
      LOG.warn("Capacity of this channel cannot be sized on the fly due " +
          "the requirement we have enough DirectMemory for the queue and " +
          "downsizing of the queue cannot be guranteed due to the " +
          "fact there maybe more items on the queue than the new capacity.");
    } else {
      capacity = newCapacity;
    }

    keepAlive =
        context.getInteger(FileChannelConfiguration.KEEP_ALIVE,
            FileChannelConfiguration.DEFAULT_KEEP_ALIVE);
    transactionCapacity =
        context.getInteger(FileChannelConfiguration.TRANSACTION_CAPACITY,
            FileChannelConfiguration.DEFAULT_TRANSACTION_CAPACITY);

    checkpointInterval =
        context.getLong(FileChannelConfiguration.CHECKPOINT_INTERVAL,
            FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL);

    // cannot be over FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE
    maxFileSize = Math.min(
        context.getLong(FileChannelConfiguration.MAX_FILE_SIZE,
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE),
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE);

    logWriteTimeout = context.getInteger(
        FileChannelConfiguration.LOG_WRITE_TIMEOUT,
        FileChannelConfiguration.DEFAULT_WRITE_TIMEOUT);

    if (logWriteTimeout < 0) {
      LOG.warn("Log write time out is invalid: " + logWriteTimeout
          + ", using default: "
          + FileChannelConfiguration.DEFAULT_WRITE_TIMEOUT);

      logWriteTimeout = FileChannelConfiguration.DEFAULT_WRITE_TIMEOUT;
    }

    checkpointWriteTimeout = context.getInteger(
        FileChannelConfiguration.CHECKPOINT_WRITE_TIMEOUT,
        FileChannelConfiguration.DEFAULT_CHECKPOINT_WRITE_TIMEOUT);

    if (checkpointWriteTimeout < 0) {
      LOG.warn("Checkpoint write time out is invalid: " + checkpointWriteTimeout
          + ", using default: "
          + FileChannelConfiguration.DEFAULT_CHECKPOINT_WRITE_TIMEOUT);

      checkpointWriteTimeout =
          FileChannelConfiguration.DEFAULT_CHECKPOINT_WRITE_TIMEOUT;
    }

    useLogReplayV1 = context.getBoolean(
        FileChannelConfiguration.USE_LOG_REPLAY_V1,
          FileChannelConfiguration.DEFAULT_USE_LOG_REPLAY_V1);

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
      builder.setQueueSize(capacity);
      builder.setLogWriteTimeout(logWriteTimeout);
      builder.setCheckpointDir(checkpointDir);
      builder.setLogDirs(dataDirs);
      builder.setChannelName(getName());
      builder.setCheckpointWriteTimeout(checkpointWriteTimeout);
      builder.setUseLogReplayV1(useLogReplayV1);
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
    Preconditions.checkState(open, "Channel closed " + channelNameDescriptor);
    FileBackedTransaction trans = transactions.get();
    if(trans != null && !trans.isClosed()) {
      Preconditions.checkState(false,
          "Thread has transaction which is still open: " +
              trans.getStateAsString()  + channelNameDescriptor);
    }
    trans = new FileBackedTransaction(log, TransactionIDOracle.next(),
        transactionCapacity, keepAlive, queueRemaining, getName(),
        channelCounter);
    transactions.set(trans);
    return trans;
  }

  int getDepth() {
    Preconditions.checkState(open, "Channel closed"  + channelNameDescriptor);
    Preconditions.checkNotNull(log, "log");
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Preconditions.checkNotNull(queue, "queue");
    return queue.getSize();
  }
  void close() {
    if(open) {
      open = false;
      log.close();
      log = null;
      queueRemaining = null;
    }
  }

  boolean isOpen() {
    return open;
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
    public FileBackedTransaction(Log log, long transactionID,
        int transCapacity, int keepAlive, Semaphore queueRemaining,
        String name, ChannelCounter counter) {
      this.log = log;
      queue = log.getFlumeEventQueue();
      this.transactionID = transactionID;
      this.keepAlive = keepAlive;
      this.queueRemaining = queueRemaining;
      putList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      takeList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
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
        throw new ChannelException("Cannot acquire capacity. "
            + channelNameDescriptor);
      }
      boolean success = false;
      boolean lockAcquired = log.tryLockShared();
      try {
        if(!lockAcquired) {
          throw new ChannelException("Failed to obtain lock for writing to the log. "
              + "Try increasing the log write timeout value or disabling it by "
              + "setting it to 0. " + channelNameDescriptor);
        }
        FlumeEventPointer ptr = log.put(transactionID, event);
        Preconditions.checkState(putList.offer(ptr), "putList offer failed "
             + channelNameDescriptor);
        queue.addWithoutCommit(ptr, transactionID);
        success = true;
      } catch (IOException e) {
        throw new ChannelException("Put failed due to IO error "
                + channelNameDescriptor, e);
      } finally {
        if(lockAcquired) {
          log.unlockShared();
        }
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
      if(!log.tryLockShared()) {
        throw new ChannelException("Failed to obtain lock for writing to the log. "
            + "Try increasing the log write timeout value or disabling it by "
            + "setting it to 0. " + channelNameDescriptor);
      }
      try {
        FlumeEventPointer ptr = queue.removeHead(transactionID);
        if(ptr != null) {
          try {
            // first add to takeList so that if write to disk
            // fails rollback actually does it's work
            Preconditions.checkState(takeList.offer(ptr), "takeList offer failed "
                 + channelNameDescriptor);
            log.take(transactionID, ptr); // write take to disk
            Event event = log.get(ptr);
            return event;
          } catch (IOException e) {
            throw new ChannelException("Take failed due to IO error "
                    + channelNameDescriptor, e);
          }
        }
        return null;
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
        if(!log.tryLockShared()) {
          throw new ChannelException("Failed to obtain lock for writing to the log. "
              + "Try increasing the log write timeout value or disabling it by "
              + "setting it to 0. " + channelNameDescriptor);
        }
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
        if(!log.tryLockShared()) {
          throw new ChannelException("Failed to obtain lock for writing to the log. "
              + "Try increasing the log write timeout value or disabling it by "
              + "setting it to 0. " + channelNameDescriptor);
        }
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
      boolean lockAcquired = log.tryLockShared();
      try {
        if(!lockAcquired) {
          throw new ChannelException("Failed to obtain lock for writing to the log. "
              + "Try increasing the log write timeout value or disabling it by "
              + "setting it to 0. " + channelNameDescriptor);
        }
        log.rollback(transactionID);
        if(takes > 0) {
          Preconditions.checkState(puts == 0, "nonzero puts and takes "
              + channelNameDescriptor);
          synchronized (queue) {
            while (!takeList.isEmpty()) {
              Preconditions.checkState(queue.addHead(takeList.removeLast()),
                  "Queue add failed, this shouldn't be able to happen "
                      + channelNameDescriptor);
            }
            queue.completeTransaction(transactionID);
          }
        }
        putList.clear();
        takeList.clear();
        channelCounter.setChannelSize(queue.getSize());
      } catch (IOException e) {
        throw new ChannelException("Commit failed due to IO error "
            + channelNameDescriptor, e);
      } finally {
        if(lockAcquired) {
          log.unlockShared();
        }
        // since rollback is being called, puts will never make it on
        // to the queue and we need to be sure to release the resources
        queueRemaining.release(puts);
      }
    }
  }
}
