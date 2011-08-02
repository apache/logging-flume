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

import org.junit.Assert;
import org.junit.Test;


public class TestFixedRetryPolicy {

  /**
   * Simple functional tests for FixedRetryPolicy
   */
  @Test
  public void testFixedRetryPolicy() {
    FixedRetryPolicy policy = new FixedRetryPolicy(3);
    Assert.assertTrue(policy.isRetryOk());
    policy.backoff();
    policy.backoff();
    policy.backoff();
    Assert.assertFalse(policy.isRetryOk());
    Assert.assertTrue(policy.isFailed());
    
    policy.reset();
    Assert.assertTrue(policy.isRetryOk());
  }
}
