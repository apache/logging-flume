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

package org.apache.flume.channel.recoverable.memory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.recoverable.memory.wal.WAL;
import org.apache.flume.channel.recoverable.memory.wal.WALEntry;
import org.apache.flume.channel.recoverable.memory.wal.WALReplayResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * <p>
 * A durable {@link Channel} implementation that uses the local file system for
 * its storage.
 * </p>
 *
 * @deprecated The RecoverableMemoryChannel has been deprecated in favor of
 * {@link org.apache.flume.channel.file.FileChannel}, which gives better
 * performance and is also durable.
 */

@Deprecated
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RecoverableMemoryChannel extends BasicChannelSemantics {

  private static final Logger LOG = LoggerFactory
      .getLogger(RecoverableMemoryChannel.class);


  public static final String WAL_DATA_DIR = "wal.dataDir";
  public static final String WAL_ROLL_SIZE = "wal.rollSize";
  public static final String WAL_MAX_LOGS_SIZE = "wal.maxLogsSize";
  public static final String WAL_MIN_RETENTION_PERIOD = "wal.minRetentionPeriod";
  public static final String WAL_WORKER_INTERVAL = "wal.workerInterval";
  public static final String CAPACITY = "capacity";
  public static final String KEEPALIVE = "keep-alive";

  public static final int DEFAULT_CAPACITY = 100;
  public static final int DEFAULT_KEEPALIVE = 3;

  private MemoryChannel memoryChannel = new MemoryChannel();
  private AtomicLong seqidGenerator = new AtomicLong(0);
  private WAL<RecoverableMemoryChannelEvent> wal;
  /**
   * MemoryChannel checks to ensure the capacity is available
   * on commit. That is a problem because we need to write to
   * disk before we commit the data to MemoryChannel. As such
   * we keep track of capacity ourselves.
   */
  private Semaphore queueRemaining;
  private int capacity;
  private int keepAlive;
  private volatile boolean open;

  public RecoverableMemoryChannel() {
    open = false;
  }

  @Override
  public void configure(Context context) {
    memoryChannel.configure(context);
    int capacity = context.getInteger(CAPACITY, DEFAULT_CAPACITY);
    if(queueRemaining == null) {
      queueRemaining = new Semaphore(capacity, true);
    } else if(capacity > this.capacity) {
      // capacity increase
      queueRemaining.release(capacity - this.capacity);
    } else if(capacity < this.capacity) {
      queueRemaining.acquireUninterruptibly(this.capacity - capacity);
    }
    this.capacity = capacity;
    keepAlive = context.getInteger(KEEPALIVE, DEFAULT_KEEPALIVE);
    long rollSize = context.getLong(WAL_ROLL_SIZE, WAL.DEFAULT_ROLL_SIZE);
    long maxLogsSize = context.getLong(WAL_MAX_LOGS_SIZE, WAL.DEFAULT_MAX_LOGS_SIZE);
    long minLogRetentionPeriod = context.getLong(WAL_MIN_RETENTION_PERIOD, WAL.DEFAULT_MIN_LOG_RETENTION_PERIOD);
    long workerInterval = context.getLong(WAL_WORKER_INTERVAL, WAL.DEFAULT_WORKER_INTERVAL);
    if(wal == null) {
      String homePath = System.getProperty("user.home").replace('\\', '/');
      String dataDir = context.getString(WAL_DATA_DIR, homePath + "/.flume/recoverable-memory-channel");
      try {
        wal = new WAL<RecoverableMemoryChannelEvent>(new File(dataDir),
            RecoverableMemoryChannelEvent.class, rollSize, maxLogsSize,
            minLogRetentionPeriod, workerInterval);
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    } else {
      wal.setRollSize(rollSize);
      wal.setMaxLogsSize(maxLogsSize);
      wal.setMinLogRetentionPeriod(minLogRetentionPeriod);
      wal.setWorkerInterval(workerInterval);
      LOG.warn(this.getClass().getSimpleName() + " only supports " +
          "partial reconfiguration.");
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting " + this);
    try {
      WALReplayResult<RecoverableMemoryChannelEvent> results = wal.replay();
      Preconditions.checkArgument(results.getSequenceID() >= 0);
      LOG.info("Replay SequenceID " + results.getSequenceID());
      seqidGenerator.set(results.getSequenceID());
      int numResults = results.getResults().size();
      Preconditions.checkState(numResults <= capacity, "Capacity " + capacity +
          ", but we need to replay " + numResults);
      LOG.info("Replay Events " + numResults);
      for(WALEntry<RecoverableMemoryChannelEvent> entry : results.getResults()) {
        seqidGenerator.set(Math.max(entry.getSequenceID(),seqidGenerator.get()));
      }
      for(WALEntry<RecoverableMemoryChannelEvent> entry : results.getResults()) {
        Transaction transaction = null;
        try {
          transaction = memoryChannel.getTransaction();
          transaction.begin();
          memoryChannel.put(entry.getData());
          transaction.commit();
        } catch(Exception e) {
          if(transaction != null) {
            try {
              transaction.rollback();
            } catch(Exception ex) {
              LOG.info("Error during rollback", ex);
            }
          }
          Throwables.propagate(e);
        } catch(Error e) {
          if(transaction != null) {
            try {
              transaction.rollback();
            } catch(Exception ex) {
              LOG.info("Error during rollback", ex);
            }
          }
          throw e;
        } finally {
          if(transaction != null) {
            transaction.close();
          }
        }
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    super.start();
    open = true;
  }

  @Override
  public synchronized void stop() {
    open = false;
    LOG.info("Stopping " + this);
    try {
      close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new RecoverableMemoryTransaction(this, memoryChannel);
  }

  private void commitEvents(List<RecoverableMemoryChannelEvent> events)
      throws IOException {
    List<WALEntry<RecoverableMemoryChannelEvent>> entries = Lists.newArrayList();
    for(RecoverableMemoryChannelEvent event : events) {
      entries.add(new WALEntry<RecoverableMemoryChannelEvent>(event, event.sequenceId));
    }
    wal.writeEntries(entries);
  }
  private void commitSequenceID(List<Long> seqids)
      throws IOException {
    wal.writeSequenceIDs(seqids);
  }

  private long nextSequenceID() {
    return seqidGenerator.incrementAndGet();
  }

  void close() throws IOException {
    if(wal != null) {
      wal.close();
    }
  }

  /**
   * <p>
   * An implementation of {@link Transaction} for {@link RecoverableMemoryChannel}s.
   * </p>
   */
  private static class RecoverableMemoryTransaction extends BasicTransactionSemantics {

    private Transaction transaction;
    private MemoryChannel memoryChannel;
    private RecoverableMemoryChannel channel;
    private List<Long> sequenceIds = Lists.newArrayList();
    private List<RecoverableMemoryChannelEvent> events = Lists.newArrayList();
    private int takes;

    private RecoverableMemoryTransaction(RecoverableMemoryChannel channel,
        MemoryChannel memoryChannel) {
      this.channel = channel;
      this.memoryChannel = memoryChannel;
      this.transaction = this.memoryChannel.getTransaction();
      this.takes = 0;
    }
    @Override
    protected void doBegin() throws InterruptedException {
      transaction.begin();
    }
    @Override
    protected void doPut(Event event) throws InterruptedException {
      if(!channel.open) {
        throw new ChannelException("Channel not open");
      }
      if(!channel.queueRemaining.tryAcquire(channel.keepAlive, TimeUnit.SECONDS)) {
        throw new ChannelException("Cannot acquire capacity");
      }
      RecoverableMemoryChannelEvent sequencedEvent =
          new RecoverableMemoryChannelEvent(event, channel.nextSequenceID());
      memoryChannel.put(sequencedEvent);
      events.add(sequencedEvent);
    }

    @Override
    protected Event doTake() throws InterruptedException {
      if(!channel.open) {
        throw new ChannelException("Channel not open");
      }
      RecoverableMemoryChannelEvent event = (RecoverableMemoryChannelEvent)memoryChannel.take();
      if(event != null) {
        sequenceIds.add(event.sequenceId);
        takes++;
        return event.event;
      }
      return null;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if(!channel.open) {
        throw new ChannelException("Channel not open");
      }
      if(sequenceIds.size() > 0) {
        try {
          channel.commitSequenceID(sequenceIds);
        } catch (IOException e) {
          throw new ChannelException("Unable to commit", e);
        }
      }
      if(!events.isEmpty()) {
        try {
          channel.commitEvents(events);
        } catch (IOException e) {
          throw new ChannelException("Unable to commit", e);
        }
      }
      transaction.commit();
      channel.queueRemaining.release(takes);
    }

    @Override
    protected void doRollback() throws InterruptedException {
      sequenceIds.clear();
      events.clear();
      channel.queueRemaining.release(events.size());
      transaction.rollback();
    }

    @Override
    protected void doClose() {
      sequenceIds.clear();
      events.clear();
      transaction.close();
    }
  }
}
