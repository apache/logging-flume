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

import java.util.Map;

import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;

/**
 * This provides a simple reusable exponential backoff state object. Note that
 * this only does calculations and tracks the backoff state but doesn't actually
 * do backoffs (e.g. do blocking sleeps)! Later this could be pluggable with
 * other backoff policies (maybe adaptive?)
 * 
 * This policy has a max backoff per attempt but *never* fails!
 */
public class CappedExponentialBackoff implements BackoffPolicy {
  final long initialSleep;
  final long sleepCap;

  // final static public String A_CUMULATIVECAP = "backoffMaxCumulativeMs";
  final static public String A_INITIAL = "backoffInitialMs";
  final static public String A_COUNT = "backoffCount";
  final static public String A_CURRENTBACKOFF = "backoffCurrentMs";
  final static public String A_RETRYTIME = "backoffRetryTime";
  final static public String A_SLEEPCAP = "backoffSleepCapMs";

  long backoffCount = 0; // number of consecutive backoff calls;
  long sleepIncrement; // amount of time before retry should happen
  long retryTime; // actual unixtime to compare against when retry is ok.

  public CappedExponentialBackoff(long initialSleep, long max) {
    this.initialSleep = initialSleep;
    this.sleepCap = max;
    reset();
  }

  /**
   * Modify state as if a backoff had just happened. Call this after failed
   * attempts.
   */
  public void backoff() {
    retryTime = Clock.unixTime() + sleepIncrement;
    sleepIncrement *= 2;
    sleepIncrement = (sleepIncrement > sleepCap) ? sleepCap : sleepIncrement;
    backoffCount++;
  }

  /**
   * Has time progressed enough to do a retry attempt?
   */
  public boolean isRetryOk() {
    return retryTime <= Clock.unixTime();
  }

  /**
   * Has so much time passed that we assume the failure is irrecoverable?
   */
  public boolean isFailed() {
    return false; // maybe a max number of attempts?
  }

  /**
   * Reset backoff state. Call this after successful attempts.
   */
  public void reset() {
    sleepIncrement = initialSleep;
    long cur = Clock.unixTime();
    retryTime = cur;
  }

  @Override
  public long sleepIncrement() {
    return sleepIncrement;
  }

  @Override
  public String getName() {
    return "CappedExpBackoff";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    rpt.setLongMetric(A_SLEEPCAP, sleepCap);
    rpt.setLongMetric(A_INITIAL, initialSleep);
    rpt.setLongMetric(A_COUNT, backoffCount);
    rpt.setLongMetric(A_CURRENTBACKOFF, sleepIncrement);
    rpt.setLongMetric(A_RETRYTIME, retryTime);
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

  /**
   * Sleep until we reach retryTime. Call isRetryOk after this returns if you
   * are concerned about backoff() being called while this thread sleeps.
   */
  @Override
  public void waitUntilRetryOk() throws InterruptedException {
    long sleeptime = retryTime - Clock.unixTime();
    if (sleeptime <= 0)
      return;
    Thread.sleep(sleeptime);
  }
}
