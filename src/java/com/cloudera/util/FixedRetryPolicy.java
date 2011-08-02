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

/**
 * Simple class that retries a fixed number of times, then fails.
 */
public class FixedRetryPolicy implements BackoffPolicy {

  protected int numRetries;
  protected int retryCount = 0;
  
  final static public String A_MAX_ATTEMPTS = "fixedmaxAttempts";
  final static public String A_COUNT = "fixedRetryCount";
  
  public FixedRetryPolicy(int numRetries) {
    this.numRetries = numRetries;
  }
  
  /**
   * Increment the retry count.
   */
  @Override
  public void backoff() {
    retryCount++;
  }

  /**
   * Returns true when the number of retry attempts exceeds the maximum.
   */
  @Override
  public boolean isFailed() {
    return retryCount >= numRetries;
  }

  /**
   * Returns true if number of retry attempts is less than the maximum allowed.
   */
  @Override
  public boolean isRetryOk() {
   return retryCount < numRetries;
  }

  @Override
  public void reset() {
    retryCount = 0;
  }

  /**
   * Returns immediately - there are no external conditions to wait for.
   */
  @Override
  public void waitUntilRetryOk() throws InterruptedException {
    // We can always retry
  }

  @Override
  public long sleepIncrement() {
    return 0;
  }

  @Override
  public String getName() {
    return "FixedRetryPolicy";
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());
    Attributes.setLong(rpt, A_MAX_ATTEMPTS, numRetries);
    Attributes.setLong(rpt, A_COUNT, retryCount);    
    return rpt;  
  }
}
