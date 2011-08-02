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
package com.cloudera.util;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.reporter.ReportEvent;

import static java.lang.Math.max;

/**
 * This provides a simple exponential backoff algorithm object. This has a
 * maxTries -- if backoff occurs than many times it moves into the failed state.
 */
public class ExponentialBackoff implements BackoffPolicy {
  final long initialSleep;
  final long maxTries;

  final static public String A_INITIAL = "backoffInitialMs";
  final static public String A_COUNT = "backoffCurCount";
  final static public String A_COUNTCAP = "backoffCountCap";
  final static public String A_CURRENTBACKOFF = "backoffCurrentMs";
  final static public String A_RETRYTIME = "backoffRetryTime";

  long backoffCount = 0; // number of consecutive backoff calls;
  long sleepIncrement; // amount of time before retry should happen
  long retryTime; // actual unixtime to compare against when retry is ok.

  public ExponentialBackoff(long initialSleep, long max) {
    this.initialSleep = initialSleep;
    this.maxTries = max;
    reset();
  }

  /**
   * Modify state as if a backoff had just happened. Call this after failed
   * attempts.
   */
  public void backoff() {
    retryTime = Clock.unixTime() + sleepIncrement;
    sleepIncrement *= 2;
    backoffCount++;
  }

  /**
   * Has time progressed enough to do a retry attempt?
   */
  public boolean isRetryOk() {
    return retryTime <= Clock.unixTime() && !isFailed();
  }

  /**
   * Has so much time passed that we assume the failure is irrecoverable?
   * 
   * If this becomes true, it will never return true on isRetryOk, until this
   * has been reset.
   */
  public boolean isFailed() {
    return backoffCount >= maxTries;
  }

  /**
   * Reset backoff state. Call this after successful attempts.
   */
  public void reset() {
    sleepIncrement = initialSleep;
    long cur = Clock.unixTime();
    retryTime = cur;
    backoffCount = 0;
  }

  @Override
  public long sleepIncrement() {
    return sleepIncrement;
  }

  @Override
  public String getName() {
    return "ExpBackoff";
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());
    Attributes.setLong(rpt, A_INITIAL, initialSleep);
    Attributes.setLong(rpt, A_COUNT, backoffCount);
    Attributes.setLong(rpt, A_COUNTCAP, maxTries);
    Attributes.setLong(rpt, A_CURRENTBACKOFF, sleepIncrement);
    Attributes.setLong(rpt, A_RETRYTIME, retryTime);
    return rpt;
  }

  /**
   * Sleep until we reach retryTime. Call isRetryOk after this returns if 
   * you are concerned about backoff() being called while this thread sleeps.
   */
  @Override
  public void waitUntilRetryOk() throws InterruptedException {    
    Thread.sleep(max(0L,retryTime - Clock.unixTime()));    
  }
}
