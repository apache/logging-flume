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

import com.cloudera.flume.reporter.Reportable;

/**
 * This is the interface for state containing backoff algorithms. Basically,
 * when a new algorithm is instantiated, reset is called. Reset should be called
 * after every successful calls. When a call fails, call backoff. This updates
 * states held by the BackoffAlgo, (such as increasing retry time). When we are
 * in a failure state, call isRetryOk to check with the backupAlgo to determine
 * if its criteria has been met to retry. (and then call reset if it succeeds).
 * 
 * Some backoff algos may want the ability to give up -- one can check this by
 * calling isFailed. If ever isFailed returns true, it will remain in that state
 * until reset is called.
 * 
 * Note that this only does calculations and tracks the backoff state but
 * doesn't actually do backoffs (e.g. do blocking sleeps)! This is intended to
 * be pluggable with other potentially more sophisticated backoff policies.
 */
public interface BackoffPolicy extends Reportable {
  /**
   * Modify state as if a backoff had just happened. Call this after failed
   * attempts.
   */
  public void backoff();

  /**
   * Has time progressed enough to do a retry attempt?
   */
  public boolean isRetryOk();

  /**
   * Has so much time passed that we assume the failure is irrecoverable?
   */
  public boolean isFailed();

  /**
   * Reset backoff state. Call this after successful attempts.
   */
  public void reset();

  /**
   * Wait time in millis until RetryOk should be true
   */
  public long sleepIncrement();

  /**
   * Wait until it's ok to retry.
   */
  public void waitUntilRetryOk() throws InterruptedException;
}
