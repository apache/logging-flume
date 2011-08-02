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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analagously to Runnable, this is a wrapper class that implements an interface
 * that is called by RetryHarness to retry an operation. 
 */
public class Retryable {
  static final Logger LOG = LoggerFactory.getLogger(Retryable.class);
  protected RetryHarness harness = null;
  
  /**
   * Called by the RetryHarness at init so that we can abort
   */
  public void setHarness(RetryHarness harness) {
    this.harness = harness;
  }
  
  /**
   * doTry is called every time a retry policy says it's ok to. Therefore
   * you should clean up all dirty state if the try fails by catching and
   * rethrowing exceptions. Return false if the attempt failed. 
   */
  public boolean doTry() throws Exception {
    throw new Exception("Subclass me!");
  }
}
