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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
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
 */
public class RecoverableMemoryChannel extends BasicChannelSemantics {

  private static final Logger LOG = LoggerFactory
      .getLogger(RecoverableMemoryChannel.class);


  public static final String WAL_DATA_DIR = "wal.dataDir";
  public static final String WAL_ROLL_SIZE = "wal.rollSize";
  public static final String WAL_MAX_LOGS_SIZE = "wal.maxLogsSize";
  public static final String WAL_MIN_RENTENTION_PERIOD = "wal.minRententionPeriod";
  public static final String WAL_WORKER_INTERVAL = "wal.workerInterval";

  private MemoryChannel memoryChannel = new MemoryChannel();
  private AtomicLong seqidGenerator = new AtomicLong(0);
  private WAL<RecoverableMemoryChannelEvent> wal;

  @Override
  public void configure(Context context) {
    memoryChannel.configure(context);

    String homePath = System.getProperty("user.home").replace('\\', '/');
    String dataDir = context.getString(WAL_DATA_DIR, homePath + "/.flume/recoverable-memory-channel");
    if(wal != null) {
      try {
        wal.close();
      } catch (IOException e) {
        LOG.error("Error closing existing wal during reconfigure", e);
      }
    }
    long rollSize = context.getLong(WAL_ROLL_SIZE, WAL.DEFAULT_ROLL_SIZE);
    long maxLogsSize = context.getLong(WAL_MAX_LOGS_SIZE, WAL.DEFAULT_MAX_LOGS_SIZE);
    long minRententionPeriod = context.getLong(WAL_MIN_RENTENTION_PERIOD, WAL.DEFAULT_MIN_LOG_RENTENTION_PERIOD);
    long workerInterval = context.getLong(WAL_WORKER_INTERVAL, WAL.DEFAULT_WORKER_INTERVAL);
    try {
      wal = new WAL<RecoverableMemoryChannelEvent>(new File(dataDir),
          RecoverableMemoryChannelEvent.class, rollSize, maxLogsSize,
          minRententionPeriod, workerInterval);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public synchronized void start() {
    try {
      WALReplayResult<RecoverableMemoryChannelEvent> results = wal.replay();
      Preconditions.checkArgument(results.getSequenceID() >= 0);
      LOG.info("Replay SequenceID " + results.getSequenceID());
      seqidGenerator.set(results.getSequenceID());
      Transaction transaction = memoryChannel.getTransaction();
      transaction.begin();
      LOG.info("Replay Events " + results.getResults().size());
      for(WALEntry<RecoverableMemoryChannelEvent> entry : results.getResults()) {
        memoryChannel.put(entry.getData());
        seqidGenerator.set(Math.max(entry.getSequenceID(),seqidGenerator.get()));
      }
      transaction.commit();
      transaction.close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    try {
      close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new FileBackedTransaction(this, memoryChannel);
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
  private static class FileBackedTransaction extends BasicTransactionSemantics {

    private Transaction transaction;
    private MemoryChannel memoryChannel;
    private RecoverableMemoryChannel fileChannel;
    private List<Long> sequenceIds = Lists.newArrayList();
    private List<RecoverableMemoryChannelEvent> events = Lists.newArrayList();
    private FileBackedTransaction(RecoverableMemoryChannel fileChannel, MemoryChannel memoryChannel) {
      this.fileChannel = fileChannel;
      this.memoryChannel = memoryChannel;
      this.transaction = this.memoryChannel.getTransaction();
    }
    @Override
    protected void doBegin() throws InterruptedException {
      transaction.begin();
    }
    @Override
    protected void doPut(Event event) throws InterruptedException {
      RecoverableMemoryChannelEvent sequencedEvent = new RecoverableMemoryChannelEvent(event, fileChannel.nextSequenceID());
      memoryChannel.put(sequencedEvent);
      events.add(sequencedEvent);
    }

    @Override
    protected Event doTake() throws InterruptedException {
      RecoverableMemoryChannelEvent event = (RecoverableMemoryChannelEvent)memoryChannel.take();
      if(event != null) {
        sequenceIds.add(event.sequenceId);
        return event.event;
      }
      return null;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if(sequenceIds.size() > 0) {
        try {
          fileChannel.commitSequenceID(sequenceIds);
        } catch (IOException e) {
          throw new ChannelException("Unable to commit", e);
        }
      }
      if(!events.isEmpty()) {
        try {
          fileChannel.commitEvents(events);
        } catch (IOException e) {
          throw new ChannelException("Unable to commit", e);
        }
      }
      transaction.commit();
    }

    @Override
    protected void doRollback() throws InterruptedException {
      sequenceIds.clear();
      events.clear();
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
