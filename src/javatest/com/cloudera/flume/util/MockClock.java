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
package com.cloudera.flume.util;

import java.util.Date;

import org.apache.log4j.Logger;

import com.cloudera.util.Clock;

/**
 * This is a wrapper and replacement for normal system calls related to time. So
 * far it support system clock and calls to Thread.sleep.
 * 
 * It uses wait() and notify() for simulating a sleep in a separate thread.
 */
public class MockClock extends Clock {
  static Logger logger = Logger.getLogger(MockClock.class.getName());

  long time = 0;
  Object lock = new Object();

  /**
   * Construct with specified start time. (in millis or unix time)
   */
  public MockClock(long start) {
    time = start;
  }

  /**
   * Get the "current" date
   */
  @Override
  public Date getDate() {
    return new Date(time);
  }

  /**
   * Get the current nanos. This is mostly used as a tie breaker if unixtime
   * doesn't have enough resolution -- subsequent class will be "slightly" after
   * earlier ones.
   */
  @Override
  public long getNanos() {
    return System.nanoTime();
  }

  /**
   * Get the "current" unix time. Note that multiple calls to this method
   * without a forward call will return the same time! Use getNanos as a tie
   * breaker.
   */
  @Override
  public long getUnixTime() {
    return time;
  }

  /**
   * Manually move time forward.
   */
  public void forward(long millis) {
    synchronized (lock) {
      time += millis;

      lock.notifyAll(); // alert any waiting sleeps
      logger.debug("running thread: " + this);
    }
    try {
      Thread.sleep(0); // yield to other threads.
    } catch (InterruptedException e) {
      // This should never happen
    }
  }

  public String toString() {
    return "virtual time: " + time;
  }

  /**
   * We simulate a sleep by checking the "time" and calling lock.wait() if the
   * "until" time hasn't been reached yet. Each call to forward sends a notify
   * which will wake the wait and check to see if it can fall through again. If
   * it cannot fall through we go back to wait.
   */
  @Override
  public void doSleep(long millis) throws InterruptedException {
    synchronized (lock) {
      long until = time + millis;

      while (this.getUnixTime() < until) {
        lock.wait(); // wait until condition is met.
      }
    }
    logger.debug("sleeping thread awoke: " + this);

  }

}
