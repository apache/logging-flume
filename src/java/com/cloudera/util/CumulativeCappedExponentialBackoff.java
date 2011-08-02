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

import com.cloudera.flume.reporter.ReportEvent;

/**
 * This provides a simple reusable exponential backoff state object. Note that
 * this only does calculations and tracks the backoff state but doesn't actually
 * do backoffs (e.g. do blocking sleeps)! Later this could be pluggable with
 * other backoff policies (maybe adaptive?)
 * 
 * This policy has a max backoff per attempt and also a cumulative maximum
 * failure time. After the cumulative time has expired isfailed will return true
 */
public class CumulativeCappedExponentialBackoff extends
    CappedExponentialBackoff {
  final long cumulativeCap;
  long failTime;

  final static public String A_CUMULATIVECAP = "backoffMaxCumulativeMs";

  public CumulativeCappedExponentialBackoff(long initialSleep, long sleepCap,
      long cumulativeCap) {
    super(initialSleep, sleepCap);
    this.cumulativeCap = cumulativeCap;
  }

  public void backoff() {
    if (backoffCount == 0) {
      failTime = Clock.unixTime() + cumulativeCap;
    }

    super.backoff();
  }

  public void reset() {
    super.reset();
    failTime = Long.MAX_VALUE;
  }

  /**
   * Has so much time passed that we assume the failure is irrecoverable?
   */
  public boolean isFailed() {
    return failTime <= Clock.unixTime();
  }

  @Override
  public String getName() {
    return "CumulativeCappedExpBackoff";
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_CUMULATIVECAP, cumulativeCap);
    return rpt;
  }
}
