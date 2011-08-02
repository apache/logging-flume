/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Main throttling Logic is here. All the choke-decorators have to call a method
 * (deleteItems) of this class before calling the append of their super class.
 * And in that method a thread is blocked if the Throttling limit either at the
 * PhysicalNode level or the choke-id level is reached. In this version, I've
 * ignored the physicalNode limit.
 */
public class ChokeManager extends Thread {

  // Time quanta in millisecs. It is a constant right now, we can change this
  // later. The main thread of the Chokemanager fills up the buckets
  // corresponding to different choke-ids and the physical node after this time
  // quanta.

  public static final int timeQuanta = 100;

  // maximum number of bytes allowed to be sent in the time quanta through a
  // physicalNode.
  /*
   * TODO(Vibhor) : In the current version, we don't use this physicalnode
   * limit, but a we are taking this value from the user, we should rectify this
   * ASAP. This should be part of a future patch dealing with some good policy
   * of dividing this physicalnode limit between different chokes.
   */
  private int physicalLimit;

  // this tells whether the ChokeManager is active or not
  private volatile boolean active = false;
  private int payLoadheadrsize = 50;
  private final HashMap<String, ChokeInfoData> chokeInfoMap = new HashMap<String, ChokeInfoData>();

  // This is the reader-writer lock on the chokeInfoMap. Whenever it is
  // being updated, a writelock has to be taken on it, and when someone is just
  // reading the map, readlock on it is sufficient.

  ReentrantReadWriteLock rwlChokeInfoMap;

  public ChokeManager() {
    super("ChokeManager");
    rwlChokeInfoMap = new ReentrantReadWriteLock();
    this.physicalLimit = Integer.MAX_VALUE;
  }

  /**
   * This method is used to change the size of setPayLoadHeaderSize.
   */
  public void setPayLoadHeaderSize(int size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException(
          "Payload header size cannot be negative");
    }
    this.payLoadheadrsize = size;
  }

  /**
   * This method is the only method used to add entries to the chokeMap
   * (chokeInfoMap). This method also assumes that a write-lock has been already
   * obtained on the Reader-Writer lock on ChokeMap (rwlChokeInfoMap).
   */
  private void register(String chokeID, int limit) {

    if (chokeInfoMap.get(chokeID) == null) {
      chokeInfoMap.put(chokeID, new ChokeInfoData(limit, chokeID));
    }
    // set a new limit if the ID was already in use
    else {
      this.chokeInfoMap.get(chokeID).setMaxLimit(limit);
    }
  }

  /**
   * This method is called in the CheckLogicalNodes method of the Liveness
   * manager. It gets the choke-d to limit mapping from the master and loads it
   * to idtoChokeInfoMap.
   */
  public void updateChokeLimitMap(Map<String, Integer> newMap) {

    rwlChokeInfoMap.writeLock().lock();
    try {
      for (String s : newMap.keySet()) {
        register(s, newMap.get(s));
      }
      // Set the PhysicalNode limit, which corresponds to entry for the ""
      // string.

      // First make sure that there is an entry for the empty key which
      // corresponds to the physicalNode limit.
      if (newMap.containsKey("")) {
        // ideally this should always true
        this.physicalLimit = newMap.get("");
      }
    } finally {
      rwlChokeInfoMap.writeLock().unlock();
    }

  }

  /**
   * This function returns true if the ChokeId passed is registered in the
   * ChokeManager.
   */
  public boolean isChokeId(String ID) {
    Boolean res;
    rwlChokeInfoMap.readLock().lock();
    try {
      res = this.chokeInfoMap.containsKey(ID);
    } finally {
      rwlChokeInfoMap.readLock().unlock();
    }
    return res;
  }

  @Override
  public void run() {
    active = true;

    while (this.active) {
      try {
        Thread.sleep(timeQuanta);
      } catch (InterruptedException e) {
        /*
         * Essentially send the control back to the beginning of the while loop.
         * If the ChokeManager is Halted(), this.active would be false and we
         * would fall out of this method.
         */
        continue;
      }

      rwlChokeInfoMap.readLock().lock();
      // the main policy logic comes here
      try {
        for (ChokeInfoData choke : this.chokeInfoMap.values()) {
          synchronized (choke) {
            choke.bucketFillup();
            choke.notifyAll();
          }
        }
      } finally {
        rwlChokeInfoMap.readLock().unlock();
      }
    }
  }

  public void halt() {
    active = false;
  }

  /**
   * This is the method a choke-decorator calls inside its append. This method
   * ensures that only the allowed number of bytes are shipped in a certain time
   * quanta. Also note that this method can block for a while but not forever.
   * This method blocks at most for 2 time quanta. So if many driver threads are
   * using the same choke and the message size is huge, accuracy can be thrown
   * off, i.e., more bytes than the maximum limit can be shipped.
   */
  public void spendTokens(String id, int numBytes) throws IOException {
    // TODO(Vibhor): Change this when we implement physical-node-level
    // throttling policy.
    rwlChokeInfoMap.readLock().lock();
    try {
      // simple policy for now: if the chokeid is not there then simply return,
      // essentially no throttling with an invalid chokeID.
      if (this.isChokeId(id) != false) {
        int loopCount = 0;
        ChokeInfoData myTinfoData = this.chokeInfoMap.get(id);
        synchronized (myTinfoData) {
          while (this.active
              && !myTinfoData.bucketCompare(numBytes + this.payLoadheadrsize)) {
            try {
              myTinfoData.wait(ChokeManager.timeQuanta);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException(e);
            }
            if (loopCount++ >= 2) // just wait twice to avoid starvation
              break;
          }
          myTinfoData.removeTokens(numBytes + this.payLoadheadrsize);
          // We are not taking the physical limit into account, that's policy
          // stuff and we'll figure this out later
        }
      }
    } finally {
      rwlChokeInfoMap.readLock().unlock();
    }
  }
}
