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
package org.apache.flume.channel;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class MemoryChannel extends BasicChannelSemantics {
  private static Logger LOGGER = LoggerFactory.getLogger(MemoryChannel.class);
  private static final Integer defaultCapacity = 100;
  private static final Integer defaultTransCapacity = 100;
  private static final Integer defaultKeepAlive = 3;

  public class MemoryTransaction extends BasicTransactionSemantics {
    private LinkedBlockingDeque<Event> takeList;
    private LinkedBlockingDeque<Event> putList;

    public MemoryTransaction(int transCapacity) {
      putList = new LinkedBlockingDeque<Event>(transCapacity);
      takeList = new LinkedBlockingDeque<Event>(transCapacity);
    }

    @Override
    protected void doPut(Event event) {
      if(!putList.offer(event)) {
        throw new ChannelException("Put queue for MemoryTransaction of capacity " +
            putList.size() + " full, consider committing more frequently, " +
            "increasing capacity or increasing thread count");
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      if(takeList.remainingCapacity() == 0) {
        throw new ChannelException("Take list for MemoryTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count");
      }
      if(!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        return null;
      }
      Event event;
      synchronized(queueLock) {
        event = queue.poll();
      }
      Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
          "signalling existence of entry");
      takeList.put(event);

      return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      int remainingChange = takeList.size() - putList.size();
      if(remainingChange < 0) {
        if(!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
          throw new ChannelException("Space for commit to queue couldn't be acquired" +
              " Sinks are likely not keeping up with sources, or the buffer size is too tight");
        }
      }
      int puts = putList.size();
      synchronized(queueLock) {
        if(puts > 0 ) {
          while(!putList.isEmpty()) {
            if(!queue.offer(putList.removeFirst())) {
              throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
            }
          }
        }
        putList.clear();
        takeList.clear();
      }
      queueStored.release(puts);
      if(remainingChange > 0) {
        queueRemaining.release(remainingChange);
      }

    }

    @Override
    protected void doRollback() {
      int takes = takeList.size();
      synchronized(queueLock) {
        Preconditions.checkState(queue.remainingCapacity() >= takeList.size(), "Not enough space in memory channel " +
            "queue to rollback takes. This should never happen, please report");
        while(!takeList.isEmpty()) {
          queue.addFirst(takeList.removeLast());
        }
        putList.clear();
      }
      queueStored.release(takes);
    }

  }

  // lock to guard queue, mainly needed to keep it locked down during resizes
  // it should never be held through a blocking operation
  private Integer queueLock;

  @GuardedBy(value = "queueLock")
  private LinkedBlockingDeque<Event> queue;

  // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
  // we maintain the remaining permits = queue.remaining - takeList.size()
  // this allows local threads waiting for space in the queue to commit without denying access to the
  // shared lock to threads that would make more space on the queue
  private Semaphore queueRemaining;
  // used to make "reservations" to grab data from the queue.
  // by using this we can block for a while to get data without locking all other threads out
  // like we would if we tried to use a blocking call on queue
  private Semaphore queueStored;
  // maximum items in a transaction queue
  private volatile Integer transCapacity;
  private volatile int keepAlive;


  public MemoryChannel() {
    super();
    queueLock = 0;
  }

  @Override
  public void configure(Context context) {
    String strCapacity = context.getString("capacity");
    Integer capacity = null;
    if(strCapacity == null) {
      capacity = defaultCapacity;
    } else {
      try {
        capacity = Integer.parseInt(strCapacity);
      } catch(NumberFormatException e) {
        capacity = defaultCapacity;
      }
    }
    String strTransCapacity = context.getString("transactionCapacity");
    if(strTransCapacity == null) {
      transCapacity = defaultTransCapacity;
    } else {
      try {
        transCapacity = Integer.parseInt(strTransCapacity);
      } catch(NumberFormatException e) {
        transCapacity = defaultTransCapacity;
      }
    }
    Preconditions.checkState(transCapacity <= capacity);

    String strKeepAlive = context.getString("keep-alive");

    if (strKeepAlive == null) {
      keepAlive = defaultKeepAlive;
    } else {
      keepAlive = Integer.parseInt(strKeepAlive);
    }

    if(queue != null) {
      try {
        resizeQueue(capacity);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      synchronized(queueLock) {
        queue = new LinkedBlockingDeque<Event>(capacity);
        queueRemaining = new Semaphore(capacity);
        queueStored = new Semaphore(0);
      }
    }
  }

  private void resizeQueue(int capacity) throws InterruptedException {
    int oldCapacity;
    synchronized(queueLock) {
      oldCapacity = queue.size() + queue.remainingCapacity();
    }

    if(oldCapacity == capacity) {
      return;
    } else if (oldCapacity > capacity) {
      if(!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
        LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
      } else {
        synchronized(queueLock) {
          LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
          newQueue.addAll(queue);
          queue = newQueue;
        }
      }
    } else {
      synchronized(queueLock) {
        LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
        newQueue.addAll(queue);
        queue = newQueue;
      }
      queueRemaining.release(capacity - oldCapacity);
    }
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new MemoryTransaction(transCapacity);
  }
}
