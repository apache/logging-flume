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
package org.apache.flume.channel;

import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.*;
import org.apache.flume.annotations.Recyclable;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.instrumentation.ChannelCounter;

import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * SpillableMemoryChannel will use main memory for buffering events until it has reached capacity.
 * Thereafter file channel will be used as overflow.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Recyclable
public class SpillableMemoryChannel extends FileChannel {
  // config settings
  /** Max number of events to be stored in memory */
  public static final String MEMORY_CAPACITY = "memoryCapacity";
  /** Seconds to wait before enabling disk overflow when memory fills up */
  public static final String OVERFLOW_TIMEOUT = "overflowTimeout";
  /** Internal use only. To remain undocumented in User guide. Determines the
   * percent free space available in mem queue when we stop spilling to overflow
   */
  public static final String OVERFLOW_DEACTIVATION_THRESHOLD
          = "overflowDeactivationThreshold";
  /** percent of buffer between byteCapacity and the estimated event size. */
  public static final String BYTE_CAPACITY_BUFFER_PERCENTAGE
          = "byteCapacityBufferPercentage";

  /** max number of bytes used for all events in the queue. */
  public static final String BYTE_CAPACITY = "byteCapacity";
  /** max number of events in overflow. */
  public static final String OVERFLOW_CAPACITY = "overflowCapacity";
  /** file channel setting that is overriden by Spillable Channel */
  public static final String KEEP_ALIVE = "keep-alive";
  /** file channel capacity overridden by Spillable Channel */
  public static final String CAPACITY = "capacity";
  /** Estimated average size of events expected to be in the channel */
  public static final String AVG_EVENT_SIZE = "avgEventSize";

  private static Logger LOGGER = LoggerFactory.getLogger(SpillableMemoryChannel.class);
  public static final int defaultMemoryCapacity = 10000;
  public static final int defaultOverflowCapacity = 100000000;

  public static final int defaultOverflowTimeout = 3;
  public static final int defaultOverflowDeactivationThreshold = 5; // percent

  // memory consumption control
  private static final int defaultAvgEventSize = 500;
  private static final Long defaultByteCapacity
          = (long)(Runtime.getRuntime().maxMemory() * .80);
  private static final int defaultByteCapacityBufferPercentage = 20;

  private volatile int byteCapacity;
  private volatile double avgEventSize = defaultAvgEventSize;
  private volatile int lastByteCapacity;
  private volatile int byteCapacityBufferPercentage;
  private Semaphore bytesRemaining;

  // for synchronizing access to primary/overflow channels & drain order
  final private Object queueLock = new Object();

  @GuardedBy(value = "queueLock")
  public ArrayDeque<Event> memQueue;

  // This semaphore tracks number of free slots in primary channel (includes
  // all active put lists) .. used to determine if the puts
  // should go into primary or overflow
  private Semaphore memQueRemaining;

  // tracks number of events in both channels. Takes will block on this
  private Semaphore totalStored;

  private int maxMemQueueSize = 0;     // max sie of memory Queue

  private boolean overflowDisabled;         // if true indicates the overflow should not be used at all.
  private boolean overflowActivated=false;  // indicates if overflow can be used. invariant: false if overflowDisabled is true.

  // if true overflow can be used. invariant: false if overflowDisabled is true.
  private int memoryCapacity = -1;     // max events that the channel can hold  in memory
  private int overflowCapacity;

  private int overflowTimeout;

  // mem full % at which we stop spill to overflow
  private double overflowDeactivationThreshold
          = defaultOverflowDeactivationThreshold / 100;

  public SpillableMemoryChannel() {
    super();
  }

  protected int getTotalStored() {
    return totalStored.availablePermits();
  }

  public int getMemoryCapacity() {
    return memoryCapacity;
  }
  public int getOverflowTimeout() {
    return overflowTimeout;
  }

  public int getMaxMemQueueSize() {
    return maxMemQueueSize;
  }

  protected Integer getOverflowCapacity() {
    return overflowCapacity;
  }

  protected boolean isOverflowDisabled() {
    return overflowDisabled;
  }

  @VisibleForTesting
  protected ChannelCounter channelCounter;

  public final DrainOrderQueue drainOrder = new DrainOrderQueue();

  public int queueSize() {
    synchronized (queueLock) {
      return memQueue.size();
    }
  }


  private static class MutableInteger {
    private int value;

    public MutableInteger(int val) {
      value = val;
    }

    public void add(int amount) {
      value += amount;
    }

    public int intValue() {
      return value;
    }
  }

  //  pop on a empty queue will throw NoSuchElementException
  // invariant: 0 will never be left in the queue

  public static class DrainOrderQueue {
    public ArrayDeque<MutableInteger> queue = new ArrayDeque<MutableInteger>(1000);

    public int totalPuts = 0;  // for debugging only
    private long overflowCounter = 0; // # of items in overflow channel

    public  String dump() {
      StringBuilder sb = new StringBuilder();

      sb.append("  [ ");
      for (MutableInteger i : queue) {
        sb.append(i.intValue());
        sb.append(" ");
      }
      sb.append("]");
      return  sb.toString();
    }

    public void putPrimary(Integer eventCount) {
      totalPuts += eventCount;
      if (  (queue.peekLast() == null) || queue.getLast().intValue() < 0) {
        queue.addLast(new MutableInteger(eventCount));
      } else {
        queue.getLast().add(eventCount);
      }
    }

    public void putFirstPrimary(Integer eventCount) {
      if ( (queue.peekFirst() == null) || queue.getFirst().intValue() < 0) {
        queue.addFirst(new MutableInteger(eventCount));
      } else {
        queue.getFirst().add(eventCount);
      }
    }

    public void putOverflow(Integer eventCount) {
      totalPuts += eventCount;
      if ( (queue.peekLast() == null) ||  queue.getLast().intValue() > 0) {
        queue.addLast(new MutableInteger(-eventCount));
      } else {
        queue.getLast().add(-eventCount);
      }
      overflowCounter += eventCount;
    }

    public void putFirstOverflow(Integer eventCount) {
      if ( (queue.peekFirst() == null) ||  queue.getFirst().intValue() > 0) {
        queue.addFirst(new MutableInteger(-eventCount));
      }  else {
        queue.getFirst().add(-eventCount);
      }
      overflowCounter += eventCount;
    }

    public int front() {
      return queue.getFirst().intValue();
    }

    public boolean isEmpty() {
      return queue.isEmpty();
    }

    public void takePrimary(int takeCount) {
      MutableInteger headValue = queue.getFirst();

      // this condition is optimization to avoid redundant conversions of
      // int -> Integer -> string in hot path
      if (headValue.intValue() < takeCount)  {
        throw new IllegalStateException("Cannot take " + takeCount +
                " from " + headValue.intValue() + " in DrainOrder Queue");
      }

      headValue.add(-takeCount);
      if (headValue.intValue() == 0) {
        queue.removeFirst();
      }
    }

    public void takeOverflow(int takeCount) {
      MutableInteger headValue = queue.getFirst();
      if(headValue.intValue() > -takeCount) {
        throw new IllegalStateException("Cannot take " + takeCount + " from "
                + headValue.intValue() + " in DrainOrder Queue head " );
      }

      headValue.add(takeCount);
      if (headValue.intValue() == 0) {
        queue.removeFirst();
      }
      overflowCounter -= takeCount;
    }

  }

  private class SpillableMemoryTransaction extends BasicTransactionSemantics {
    BasicTransactionSemantics overflowTakeTx = null; // Take-Txn for overflow
    BasicTransactionSemantics overflowPutTx = null;  // Put-Txn for overflow
    boolean useOverflow = false;
    boolean putCalled = false;    // set on first invocation to put
    boolean takeCalled = false;   // set on first invocation to take
    int largestTakeTxSize = 5000; // not a constraint, just hint for allocation
    int largestPutTxSize = 5000;  // not a constraint, just hint for allocation

    Integer overflowPutCount = 0;    // # of puts going to overflow in this Txn

    private int putListByteCount = 0;
    private int takeListByteCount = 0;
    private int takeCount = 0;

    ArrayDeque<Event> takeList;
    ArrayDeque<Event> putList;
    private final ChannelCounter channelCounter;


    public SpillableMemoryTransaction(ChannelCounter counter) {
      takeList = new ArrayDeque<Event>(largestTakeTxSize);
      putList = new ArrayDeque<Event>(largestPutTxSize);
      channelCounter = counter;
    }

    @Override
    public void begin() {
      super.begin();
    }

    @Override
    public void close() {
      if (overflowTakeTx!=null) {
        overflowTakeTx.close();
      }
      if (overflowPutTx!=null) {
        overflowPutTx.close();
      }
      super.close();
    }


    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();

      putCalled = true;
      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/ avgEventSize);
      if (!putList.offer(event)) {
        throw new ChannelFullException("Put queue in " + getName() +
                " channel's Transaction having capacity " + putList.size() +
                " full, consider reducing batch size of sources");
      }
      putListByteCount += eventByteSize;
    }


    // Take will limit itself to a single channel within a transaction.
    // This ensures commits/rollbacks are restricted to a single channel.
    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      if (!totalStored.tryAcquire(overflowTimeout, TimeUnit.SECONDS)) {
        LOGGER.debug("Take is backing off as channel is empty.");
        return null;
      }
      boolean takeSuceeded = false;
      try {
        Event event;
        synchronized(queueLock) {
          int drainOrderTop = drainOrder.front();

          if (!takeCalled) {
            takeCalled = true;
            if (drainOrderTop < 0) {
              useOverflow = true;
              overflowTakeTx = getOverflowTx();
              overflowTakeTx.begin();
            }
          }

          if (useOverflow) {
            if (drainOrderTop > 0) {
              LOGGER.debug("Take is switching to primary");
              return null;       // takes should now occur from primary channel
            }

            event = overflowTakeTx.take();
            ++takeCount;
            drainOrder.takeOverflow(1);
          } else {
            if (drainOrderTop < 0) {
              LOGGER.debug("Take is switching to overflow");
              return null;      // takes should now occur from overflow channel
            }

            event = memQueue.poll();
            ++takeCount;
            drainOrder.takePrimary(1);
            Preconditions.checkNotNull(event, "Queue.poll returned NULL despite"
                    + " semaphore signalling existence of entry");
          }
        }

        int eventByteSize = (int)Math.ceil(estimateEventSize(event)/ avgEventSize);
        if (!useOverflow) {
          // takeList is thd pvt, so no need to do this in synchronized block
          takeList.offer(event);
        }

        takeListByteCount += eventByteSize;
        takeSuceeded = true;
        return event;
      } finally {
        if(!takeSuceeded) {
          totalStored.release();
        }
      }
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (putCalled) {
        putCommit();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Put Committed. Drain Order Queue state : "
                  + drainOrder.dump());
        }
      } else if (takeCalled) {
        takeCommit();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Take Committed. Drain Order Queue state : "
                  + drainOrder.dump());
        }
      }
    }

    private void takeCommit() {
      if (takeCount > largestTakeTxSize)
        largestTakeTxSize = takeCount;

      synchronized (queueLock) {
        if (overflowTakeTx!=null) {
          overflowTakeTx.commit();
        }
        double memoryPercentFree = (memoryCapacity == 0) ?  0
           :  (memoryCapacity - memQueue.size() + takeCount ) / (double)memoryCapacity ;

        if (overflowActivated
                &&  memoryPercentFree >= overflowDeactivationThreshold) {
          overflowActivated = false;
          LOGGER.info("Overflow Deactivated");
        }
        channelCounter.setChannelSize(getTotalStored());
      }
      if (!useOverflow)  {
        memQueRemaining.release(takeCount);
        bytesRemaining.release(takeListByteCount);
      }

      channelCounter.addToEventTakeSuccessCount(takeCount);
    }

    private void putCommit() throws InterruptedException {
      // decide if overflow needs to be used
      int timeout = overflowActivated  ? 0 : overflowTimeout;

      if (memoryCapacity != 0) {
        // check for enough event slots(memoryCapacity) for using memory queue
        if (!memQueRemaining.tryAcquire(putList.size(), timeout,
          TimeUnit.SECONDS)) {
          if (overflowDisabled) {
            throw new ChannelFullException("Spillable Memory Channel's " +
              "memory capacity has been reached and overflow is " +
              "disabled. Consider increasing memoryCapacity.");
          }
          overflowActivated = true;
          useOverflow = true;
        }
        // check if we have enough byteCapacity for using memory queue
        else if (!bytesRemaining.tryAcquire(putListByteCount, overflowTimeout
          , TimeUnit.SECONDS)) {
          memQueRemaining.release(putList.size());
          if (overflowDisabled) {
            throw new ChannelFullException("Spillable Memory Channel's "
              + "memory capacity has been reached.  "
              + (bytesRemaining.availablePermits() * (int) avgEventSize)
              + " bytes are free and overflow is disabled. Consider "
              + "increasing byteCapacity or capacity.");
          }
          overflowActivated = true;
          useOverflow = true;
        }
      } else {
        useOverflow = true;
      }

      if (putList.size() > largestPutTxSize) {
        largestPutTxSize = putList.size();
      }

      if (useOverflow) {
        commitPutsToOverflow();
      } else {
        commitPutsToPrimary();
      }
    }

    private void commitPutsToOverflow() throws InterruptedException {
      overflowPutTx = getOverflowTx();
      overflowPutTx.begin();
      for (Event event : putList) {
        overflowPutTx.put(event);
      }
      commitPutsToOverflow_core(overflowPutTx);
      totalStored.release(putList.size());
      overflowPutCount += putList.size();
      channelCounter.addToEventPutSuccessCount(putList.size());
    }

    private void commitPutsToOverflow_core(Transaction overflowPutTx)
            throws InterruptedException {
      // reattempt only once if overflow is full first time around
      for (int i = 0; i < 2; ++i)  {
        try {
          synchronized(queueLock) {
            overflowPutTx.commit();
            drainOrder.putOverflow(putList.size());
            channelCounter.setChannelSize(memQueue.size()
                    + drainOrder.overflowCounter);
            break;
          }
        } catch (ChannelFullException e)  { // drop lock & reattempt
          if (i==0) {
            Thread.sleep(overflowTimeout *1000);
          }
          else {
            throw e;
          }
        }
      }
    }

    private void commitPutsToPrimary() {
      synchronized (queueLock) {
        for (Event e : putList) {
          if (!memQueue.offer(e)) {
            throw new ChannelException("Unable to insert event into memory " +
                    "queue in spite of spare capacity, this is very unexpected");
          }
        }
        drainOrder.putPrimary(putList.size());
        maxMemQueueSize = (memQueue.size() > maxMemQueueSize) ?  memQueue.size()
                                                              : maxMemQueueSize;
        channelCounter.setChannelSize(memQueue.size()
                + drainOrder.overflowCounter);
      }
      // update counters and semaphores
      totalStored.release(putList.size());
      channelCounter.addToEventPutSuccessCount(putList.size());
    }

    @Override
    protected void doRollback() {
      LOGGER.debug("Rollback() of " +
              (takeCalled ? " Take Tx" : (putCalled ? " Put Tx" : "Empty Tx")));

      if (putCalled) {
        if (overflowPutTx!=null) {
          overflowPutTx.rollback();
        }
        if (!useOverflow) {
          bytesRemaining.release(putListByteCount);
          putList.clear();
        }
        putListByteCount = 0;
      } else if (takeCalled) {
        synchronized(queueLock) {
          if (overflowTakeTx!=null) {
            overflowTakeTx.rollback();
          }
          if (useOverflow) {
            drainOrder.putFirstOverflow(takeCount);
          } else {
            int remainingCapacity = memoryCapacity - memQueue.size();
            Preconditions.checkState(remainingCapacity >= takeCount,
                    "Not enough space in memory queue to rollback takes. This" +
                            " should never happen, please report");
            while (!takeList.isEmpty()) {
              memQueue.addFirst(takeList.removeLast());
            }
            drainOrder.putFirstPrimary(takeCount);
          }
        }
        totalStored.release(takeCount);
      } else {
        overflowTakeTx.rollback();
      }
      channelCounter.setChannelSize(memQueue.size() + drainOrder.overflowCounter);
    }
  } // Transaction

  /**
   * Read parameters from context
   * <li>memoryCapacity = total number of events allowed at one time in the memory queue.
   * <li>overflowCapacity = total number of events allowed at one time in the overflow file channel.
   * <li>byteCapacity = the max number of bytes used for events in the memory queue.
   * <li>byteCapacityBufferPercentage = type int. Defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>overflowTimeout = type int. Number of seconds to wait on a full memory before deciding to enable overflow
   */
  @Override
  public void configure(Context context) {

    if (getLifecycleState() == LifecycleState.START    // does not support reconfig when running
            || getLifecycleState() == LifecycleState.ERROR)
      stop();

    if (totalStored == null) {
      totalStored = new Semaphore(0);
    }

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }

    // 1) Memory Capacity
    Integer newMemoryCapacity;
    try {
      newMemoryCapacity = context.getInteger(MEMORY_CAPACITY
              , defaultMemoryCapacity);
      if (newMemoryCapacity == null) {
        newMemoryCapacity = defaultMemoryCapacity;
      }
      if (newMemoryCapacity < 0) {
        throw new NumberFormatException(MEMORY_CAPACITY + " must be >= 0");
      }

    } catch(NumberFormatException e) {
      newMemoryCapacity = defaultMemoryCapacity;
      LOGGER.warn("Invalid " + MEMORY_CAPACITY + " specified, initializing " +
          getName() + " channel to default value of {}", defaultMemoryCapacity);
    }
    try {
      resizePrimaryQueue(newMemoryCapacity);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // overflowTimeout - wait time before switching to overflow when mem is full
    try {
      Integer newOverflowTimeout =
              context.getInteger(OVERFLOW_TIMEOUT, defaultOverflowTimeout);
      overflowTimeout = (newOverflowTimeout != null) ? newOverflowTimeout
                                                     : defaultOverflowTimeout;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect value for " + getName() + "'s " + OVERFLOW_TIMEOUT
            + " setting. Using default value {}", defaultOverflowTimeout);
      overflowTimeout = defaultOverflowTimeout;
    }

    try {
      Integer newThreshold = context.getInteger(OVERFLOW_DEACTIVATION_THRESHOLD);
      overflowDeactivationThreshold =  (newThreshold != null) ?
                                   newThreshold/100.0
                                 : defaultOverflowDeactivationThreshold / 100.0;
    } catch(NumberFormatException e) {
      LOGGER.warn("Incorrect value for " + getName() + "'s " +
              OVERFLOW_DEACTIVATION_THRESHOLD + ". Using default value {} %",
              defaultOverflowDeactivationThreshold);
      overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100.0;
    }

    // 3) Memory consumption control
    try {
      byteCapacityBufferPercentage =
              context.getInteger(BYTE_CAPACITY_BUFFER_PERCENTAGE
                      , defaultByteCapacityBufferPercentage);
    } catch(NumberFormatException e) {
      LOGGER.warn("Error parsing " + BYTE_CAPACITY_BUFFER_PERCENTAGE + " for "
              + getName() + ". Using default="
              + defaultByteCapacityBufferPercentage + ". " + e.getMessage());
      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
    }

    try {
      avgEventSize = context.getInteger(AVG_EVENT_SIZE, defaultAvgEventSize);
    } catch ( NumberFormatException e) {
      LOGGER.warn("Error parsing " + AVG_EVENT_SIZE + " for " + getName()
              + ". Using default = " + defaultAvgEventSize + ". "
              + e.getMessage());
      avgEventSize = defaultAvgEventSize;
    }

    try {
      byteCapacity = (int) ((context.getLong(BYTE_CAPACITY, defaultByteCapacity) * (1 - byteCapacityBufferPercentage * .01 )) / avgEventSize);
      if (byteCapacity < 1) {
        byteCapacity = Integer.MAX_VALUE;
      }
    } catch(NumberFormatException e) {
      LOGGER.warn("Error parsing " + BYTE_CAPACITY + " setting for " + getName()
              + ". Using default = " + defaultByteCapacity + ". "
              + e.getMessage());
      byteCapacity = (int)
            ( (defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 ))
                    / avgEventSize);
    }


    if (bytesRemaining == null) {
      bytesRemaining = new Semaphore(byteCapacity);
      lastByteCapacity = byteCapacity;
    } else {
      if (byteCapacity > lastByteCapacity) {
        bytesRemaining.release(byteCapacity - lastByteCapacity);
        lastByteCapacity = byteCapacity;
      } else {
        try {
          if (!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, overflowTimeout, TimeUnit.SECONDS)) {
            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
          } else {
            lastByteCapacity = byteCapacity;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    try {
      overflowCapacity = context.getInteger(OVERFLOW_CAPACITY, defaultOverflowCapacity);  // file channel capacity
      // Determine if File channel needs to be disabled
        if ( memoryCapacity < 1   &&   overflowCapacity < 1) {
          LOGGER.warn("For channel " + getName() + OVERFLOW_CAPACITY +
                  " cannot be set to 0 if " + MEMORY_CAPACITY + " is also 0. " +
                  "Using default value " + OVERFLOW_CAPACITY + " = " +
                  defaultOverflowCapacity);
          overflowCapacity = defaultOverflowCapacity;
        }
        overflowDisabled = (overflowCapacity < 1) ;
        if (overflowDisabled) {
          overflowActivated = false;
        }
    } catch(NumberFormatException e) {
      overflowCapacity = defaultOverflowCapacity;
    }

    // Configure File channel
    context.put(KEEP_ALIVE,"0"); // override keep-alive for  File channel
    context.put(CAPACITY, Integer.toString(overflowCapacity) );  // file channel capacity
    super.configure(context);
  }


  private void resizePrimaryQueue(int newMemoryCapacity) throws InterruptedException {
    if (memQueue != null   &&   memoryCapacity == newMemoryCapacity) {
      return;
    }

    if (memoryCapacity > newMemoryCapacity) {
      int diff = memoryCapacity - newMemoryCapacity;
      if (!memQueRemaining.tryAcquire(diff, overflowTimeout, TimeUnit.SECONDS)) {
        LOGGER.warn("Memory buffer currently contains more events than the new size. Downsizing has been aborted.");
        return;
      }
      synchronized(queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        newQueue.addAll(memQueue);
        memQueue = newQueue;
        memoryCapacity = newMemoryCapacity;
      }
    } else  {   // if (memoryCapacity <= newMemoryCapacity)
      synchronized(queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        if (memQueue !=null) {
          newQueue.addAll(memQueue);
        }
        memQueue = newQueue;
        if (memQueRemaining == null) {
          memQueRemaining = new Semaphore(newMemoryCapacity);
        } else {
          int diff = newMemoryCapacity - memoryCapacity;
          memQueRemaining.release(diff);
        }
        memoryCapacity = newMemoryCapacity;
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    int overFlowCount = super.getDepth();
    if (drainOrder.isEmpty()) {
      drainOrder.putOverflow(overFlowCount);
      totalStored.release(overFlowCount);
    }
    int totalCount =  overFlowCount + memQueue.size();
    channelCounter.setChannelCapacity(memoryCapacity + getOverflowCapacity());
    channelCounter.setChannelSize(totalCount);
  }

  @Override
  public synchronized void stop() {
    if (getLifecycleState()==LifecycleState.STOP) {
      return;
    }
    channelCounter.setChannelSize(memQueue.size() + drainOrder.overflowCounter);
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new SpillableMemoryTransaction(channelCounter);
  }


  private BasicTransactionSemantics getOverflowTx() {
    return super.createTransaction();
  }

  private long estimateEventSize(Event event) {
    byte[] body = event.getBody();
    if (body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }

}
