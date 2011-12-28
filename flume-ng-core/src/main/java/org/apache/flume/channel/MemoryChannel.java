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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;

import com.google.common.base.Preconditions;

/**
 * Memory channel that with full transaction support Uses transaction object for
 * each thread (source and sink) attached to channel. The events are stored in
 * the thread safe Dequeue. * The put and take are directly executed in the
 * common queue. Channel has a marker for the last committed event in order to
 * avoid sink reading uncommitted data. The transactions keep track of the
 * actions to perform undo when rolled back.
 * 
 */
public class MemoryChannel implements Channel, Configurable {

  private static final Integer defaultCapacity = 50;
  private static final Integer defaultKeepAlive = 3;

  // wrap the event with a counter
  private class StampedEvent {
    private int timeStamp;
    private Event event;

    public StampedEvent(int stamp, Event E) {
      timeStamp = stamp;
      event = E;
    }

    public int getStamp() {
      return timeStamp;
    }

    public Event getEvent() {
      return event;
    }

  }

  /*
   * transaction class maintain a 'undo' list for put/take from the queue. The
   * rollback performs undo of the operations using these lists. Also maintain a
   * stamp/counter for commit and last take. This is used to ensure that a
   * transaction doesn't read uncommitted events.
   */
  public class MemTransaction implements Transaction {
    private int putStamp;
    private int takeStamp;
    private LinkedList<StampedEvent> undoTakeList;
    private LinkedList<StampedEvent> undoPutList;
    private TransactionState txnState;
    private int refCount;

    public MemTransaction() {
      txnState = TransactionState.Closed;
    }

    @Override
    /** 
     * Start the transaction 
     *  initialize the undo lists, stamps
     *  set transaction state to Started
     */
    public void begin() {
      if (++refCount > 1) {
        return;
      }
      undoTakeList = new LinkedList<StampedEvent>();
      undoPutList = new LinkedList<StampedEvent>();
      putStamp = 0;
      takeStamp = 0;
      txnState = TransactionState.Started;
    }

    @Override
    /**
     * Commit the transaction
     *  If there was an event added by this transaction, then set the 
     *  commit stamp set transaction state to Committed
     */
    public void commit() {
      Preconditions.checkArgument(txnState == TransactionState.Started,
          "transaction not started");
      if (--refCount > 0) {
        return;
      }

      // if the txn put any events, then update the channel's stamp and
      // signal for availability of committed data in the queue
      if (putStamp != 0) {
        lastCommitStamp.set(putStamp);
        lock.lock();
        try {
          hasData.signal();
        } finally {
          lock.unlock();
        }
      }
      txnState = TransactionState.Committed;
      undoPutList.clear();
      undoTakeList.clear();
    }

    @Override
    /**
     * Rollback the transaction
     *  execute the channel's undoXXX to undo the actions done by this txn
     *  set transaction state to rolled back
     */
    public void rollback() {
      Preconditions.checkArgument(txnState == TransactionState.Started,
          "transaction not started");
      undoPut(this);
      undoTake(this);
      txnState = TransactionState.RolledBack;
      refCount = 0;
    }

    @Override
    /** 
     * Close the transaction
     *  if the transaction is still open, then roll it back
     *  set transaction state to Closed
     */
    public void close() {
      if (txnState == TransactionState.Started) {
        rollback();
      }
      txnState = TransactionState.Closed;
      forgetTransaction(this);
    }

    public TransactionState getState() {
      return txnState;
    }

    protected int lastTakeStamp() {
      return takeStamp;
    }

    protected void logPut(StampedEvent e, int stamp) {
      undoPutList.addLast(e);
      putStamp = stamp;
    }

    protected void logTake(StampedEvent e, int stamp) {
      undoTakeList.addLast(e);
      takeStamp = stamp;
    }

    protected StampedEvent removePut() {
      if (undoPutList.isEmpty()) {
        return null;
      } else {
        return undoPutList.removeLast();
      }
    }

    protected StampedEvent removeTake() {
      if (undoTakeList.isEmpty()) {
        return null;
      } else {
        return undoTakeList.removeLast();
      }
    }

  }

  // The main event queue
  private LinkedBlockingDeque<StampedEvent> queue;

  private AtomicInteger currentStamp; // operation counter
  private AtomicInteger lastCommitStamp; // counter for the last commit
  private ConcurrentHashMap<Long, MemTransaction> txnMap; // open transactions
  private Integer keepAlive;
  final Lock lock = new ReentrantLock();
  final Condition hasData = lock.newCondition();

  /**
   * Channel constructor
   */
  public MemoryChannel() {
    currentStamp = new AtomicInteger(1);
    lastCommitStamp = new AtomicInteger(0);
    txnMap = new ConcurrentHashMap<Long, MemTransaction>();
  }

  /**
   * set the event queue capacity
   */
  @Override
  public void configure(Context context) {
    String strCapacity = context.get("capacity", String.class);
    Integer capacity = null;

    if (strCapacity == null) {
      capacity = defaultCapacity;
    } else {
      capacity = Integer.parseInt(strCapacity);
    }

    String strKeepAlive = context.get("keep-alive", String.class);

    if (strKeepAlive == null) {
      keepAlive = defaultKeepAlive;
    } else {
      keepAlive = Integer.parseInt(strKeepAlive);
    }

    queue = new LinkedBlockingDeque<StampedEvent>(capacity);
  }

  @Override
  /** 
   * Add the given event to the end of the queue
   * save the event in the undoPut queue for possible rollback
   * save the stamp of this put for commit
   */
  public void put(Event event) {
    Preconditions.checkState(queue != null,
        "No queue defined (Did you forget to configure me?");

    try {
      MemTransaction myTxn = findTransaction();
      Preconditions.checkState(myTxn != null, "Transaction not started");

      int myStamp = currentStamp.getAndIncrement();
      StampedEvent stampedEvent = new StampedEvent(myStamp, event);
      if (queue.offer(stampedEvent,keepAlive, TimeUnit.SECONDS) == false)
        throw new ChannelException("put(" + event + ") timed out");
      myTxn.logPut(stampedEvent, myStamp);

    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to put(" + event + ")", ex);
    }
  }

  /**
   * undo of put for all the events in the undoPut queue, remove those from the
   * event queue
   * 
   * @param myTxn
   */
  protected void undoPut(MemTransaction myTxn) {
    StampedEvent undoEvent;
    StampedEvent currentEvent;

    while ((undoEvent = myTxn.removePut()) != null) {
      currentEvent = queue.removeLast();
      Preconditions.checkNotNull(currentEvent, "Rollback error");
      Preconditions.checkArgument(currentEvent == undoEvent, "Rollback error");
    }
  }

  @Override
  /**
   * remove the event from the top of the queue and return it
   * also add that event to undoTake queue for possible rollback
   */
  public Event take() {
    Preconditions.checkState(queue != null, "Queue not configured");

    try {
      MemTransaction myTxn = findTransaction();
      Preconditions.checkState(myTxn != null, "Transaction not started");
      Event event = null;
      int timeout = keepAlive;

      // wait for some committed data be there in the queue
      if ((timeout > 0) && (myTxn.lastTakeStamp() == lastCommitStamp.get())) {
        lock.lock();
        try {
          hasData.await(timeout, TimeUnit.SECONDS);
        } finally {
          lock.unlock();
        }
        timeout = 0; // don't wait any further
      }

      // don't go past the last committed element
      if (myTxn.lastTakeStamp() != lastCommitStamp.get()) {
        StampedEvent e = queue.poll(timeout, TimeUnit.SECONDS);
        if (e != null) {
          myTxn.logTake(e, e.getStamp());
          event = e.getEvent();
        }
      }
      return event;
    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to take()", ex);
    }
  }

  /**
   * undo of take operation for each event in the undoTake list, add it back to
   * the event queue
   * 
   * @param myTxn
   */
  protected void undoTake(MemTransaction myTxn) {
    StampedEvent e;

    while ((e = myTxn.removeTake()) != null) {
      queue.addFirst(e);
    }
  }

  @Override
  /**
   * Return the channel's transaction
   */
  public Transaction getTransaction() {
    MemTransaction txn;

    // check if there's already a transaction created for this thread
    txn = findTransaction();

    // Create a new transaction
    if (txn == null) {
      txn = new MemTransaction();
      txnMap.put(Thread.currentThread().getId(), txn);
    }
    return txn;
  }

  /**
   * Remove the given transaction from the list of open transactions
   * 
   * @param myTxn
   */
  protected void forgetTransaction(MemTransaction myTxn) {
    MemTransaction currTxn = findTransaction();
    Preconditions.checkArgument(myTxn == currTxn, "Wrong transaction to close");
    txnMap.remove(Thread.currentThread().getId());
  }

  // lookup the transaction for the current thread
  protected MemTransaction findTransaction() {
    try {
      return txnMap.get(Thread.currentThread().getId());
    } catch (NullPointerException eN) {
      return null;
    }
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }
}
