/*
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
package org.apache.flume.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A basic implementation of an order selector that implements a simple
 * exponential backoff algorithm. Subclasses can use the same algorithm for
 * backoff by simply overriding <tt>createIterator</tt> method to order the
 * list of active sinks returned by <tt>getIndexList</tt> method. Classes
 * instantiating subclasses of this class are expected to call <tt>informFailure</tt>
 * method when an object passed to this class should be marked as failed and backed off.
 *
 * When implementing a different backoff algorithm, a subclass should
 * minimally override <tt>informFailure</tt> and <tt>getIndexList</tt> methods.
 *
 * @param <T> - The class on which ordering is to be done
 */
public abstract class OrderSelector<T> {

  private static final int EXP_BACKOFF_COUNTER_LIMIT = 16;
  private static final long CONSIDER_SEQUENTIAL_RANGE = TimeUnit.HOURS
    .toMillis(1);
  private static final long MAX_TIMEOUT = 30000l;
  private final Map<T, FailureState> stateMap =
          new LinkedHashMap<T, FailureState>();
  private long maxTimeout = MAX_TIMEOUT;
  private final boolean shouldBackOff;

  protected OrderSelector(boolean shouldBackOff) {
    this.shouldBackOff = shouldBackOff;
  }

  /**
   * Set the list of objects which this class should return in order.
   * @param objects
   */
  @SuppressWarnings("unchecked")
  public void setObjects(List<T> objects) {
    //Order is the same as the original order.

    for (T sink : objects) {
      FailureState state = new FailureState();
      stateMap.put(sink, state);
    }
  }

  /**
   * Get the list of objects to be ordered. This list is in the same order
   * as originally passed in, not in the algorithmically reordered order.
   * @return - list of objects to be ordered.
   */
  public List<T> getObjects() {
    return new ArrayList<T>(stateMap.keySet());
  }

  /**
   *
   * @return - list of algorithmically ordered active sinks
   */
  public abstract Iterator<T> createIterator();

  /**
   * Inform this class of the failure of an object so it can be backed off.
   * @param failedObject
   */
  public void informFailure(T failedObject) {
    //If there is no backoff this method is a no-op.
    if (!shouldBackOff) {
      return;
    }
    FailureState state = stateMap.get(failedObject);
    long now = System.currentTimeMillis();
    long delta = now - state.lastFail;

    /*
     * When do we increase the backoff period?
     * We basically calculate the time difference between the last failure
     * and the current one. If this failure happened within one hour of the
     * last backoff period getting over, then we increase the timeout,
     * since the object did not recover yet. Else we assume this is a fresh
     * failure and reset the count.
     */
    long lastBackoffLength = Math.min(maxTimeout, 1000 * (1 << state.sequentialFails));
    long allowableDiff = lastBackoffLength + CONSIDER_SEQUENTIAL_RANGE;
    if (allowableDiff > delta) {
      if (state.sequentialFails < EXP_BACKOFF_COUNTER_LIMIT) {
        state.sequentialFails++;
      }
    } else {
      state.sequentialFails = 1;
    }
    state.lastFail = now;
    //Depending on the number of sequential failures this component had, delay
    //its restore time. Each time it fails, delay the restore by 1000 ms,
    //until the maxTimeOut is reached.
    state.restoreTime = now + Math.min(maxTimeout, 1000 * (1 << state.sequentialFails));
  }

  /**
   *
   * @return - List of indices currently active objects
   */
  protected List<Integer> getIndexList() {
    long now = System.currentTimeMillis();

    List<Integer> indexList = new ArrayList<Integer>();

    int i = 0;
    for (T obj : stateMap.keySet()) {
      if (!isShouldBackOff() || stateMap.get(obj).restoreTime < now) {
        indexList.add(i);
      }
      i++;
    }
    return indexList;
  }

  public boolean isShouldBackOff() {
    return shouldBackOff;
  }

  public void setMaxTimeOut(long timeout) {
    this.maxTimeout = timeout;
  }

  public long getMaxTimeOut() {
    return this.maxTimeout;
  }

  private static class FailureState {
    long lastFail = 0;
    long restoreTime = 0;
    int sequentialFails = 0;
  }
}
