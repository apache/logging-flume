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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.Log.Builder;
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
  private String channelNameDescriptor = "[channel=unknown]";

  @Override
  public synchronized void setName(String name) {
    channelNameDescriptor = "[channel=" + name + "]";
    super.setName(name);
  }

  /**
   * Transaction IDs should unique within a file channel
   * across JVM restarts.
   */
  private static final AtomicLong TRANSACTION_ID =
      new AtomicLong(System.currentTimeMillis());

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


    if(queueRemaining == null) {
      queueRemaining = new Semaphore(capacity, true);
    }
    if(log != null) {
      log.setCheckpointInterval(checkpointInterval);
      log.setMaxFileSize(maxFileSize);
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

      log = builder.build();

      log.replay();
      open = true;

      int depth = getDepth();
      Preconditions.checkState(queueRemaining.tryAcquire(depth),
          "Unable to acquire " + depth + " permits " + channelNameDescriptor);
      LOG.info("Queue Size after replay: " + depth + " "
           + channelNameDescriptor);
    } catch (Exception ex) {
      open = false;
      LOG.error("Failed to start the file channel", ex);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}...", this);
    close();
    super.stop();
  }

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
    trans = new FileBackedTransaction(log, TRANSACTION_ID.incrementAndGet(),
        transactionCapacity, keepAlive, queueRemaining, getName());
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
    public FileBackedTransaction(Log log, long transactionID,
        int transCapacity, int keepAlive, Semaphore queueRemaining,
        String name) {
      this.log = log;
      queue = log.getFlumeEventQueue();
      this.transactionID = transactionID;
      this.keepAlive = keepAlive;
      this.queueRemaining = queueRemaining;
      putList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      takeList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      channelNameDescriptor = "[channel=" + name + "]";
    }
    private boolean isClosed() {
      return State.CLOSED.equals(getState());
    }
    private String getStateAsString() {
      return String.valueOf(getState());
    }
    @Override
    protected void doPut(Event event) throws InterruptedException {
      if(putList.remainingCapacity() == 0) {
        throw new ChannelException("Put queue for FileBackedTransaction " +
            "of capacity " + putList.size() + " full, consider " +
            "committing more frequently, increasing capacity or " +
            "increasing thread count. " + channelNameDescriptor);
      }
      if(!queueRemaining.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        throw new ChannelException("Cannot acquire capacity. "
            + channelNameDescriptor);
      }
      try {
        FlumeEventPointer ptr = log.put(transactionID, event);
        Preconditions.checkState(putList.offer(ptr), "putList offer failed "
             + channelNameDescriptor);
      } catch (IOException e) {
        throw new ChannelException("Put failed due to IO error "
                + channelNameDescriptor, e);
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      if(takeList.remainingCapacity() == 0) {
        throw new ChannelException("Take list for FileBackedTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count. "
               + channelNameDescriptor);
      }
      FlumeEventPointer ptr = queue.removeHead();
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
    }

    @Override
    protected void doCommit() throws InterruptedException {
      int puts = putList.size();
      int takes = takeList.size();
      if(puts > 0) {
        Preconditions.checkState(takes == 0, "nonzero puts and takes "
                + channelNameDescriptor);
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
        }
        try {
          log.commitPut(transactionID);
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
              + channelNameDescriptor, e);
        }
      } else if(takes > 0) {
        try {
          log.commitTake(transactionID);
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
               + channelNameDescriptor, e);
        }
        queueRemaining.release(takes);
      }
      putList.clear();
      takeList.clear();
    }

    @Override
    protected void doRollback() throws InterruptedException {
      int puts = putList.size();
      int takes = takeList.size();
      if(takes > 0) {
        Preconditions.checkState(puts == 0, "nonzero puts and takes "
            + channelNameDescriptor);
        while(!takeList.isEmpty()) {
          Preconditions.checkState(queue.addHead(takeList.removeLast()),
              "Queue add failed, this shouldn't be able to happen "
                   + channelNameDescriptor);
        }
      }
      queueRemaining.release(puts);
      try {
        log.rollback(transactionID);
      } catch (IOException e) {
        throw new ChannelException("Commit failed due to IO error "
             + channelNameDescriptor, e);
      }
      putList.clear();
      takeList.clear();
    }

  }
}
