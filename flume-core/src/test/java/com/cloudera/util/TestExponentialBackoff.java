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

import com.cloudera.flume.util.MockClock;

/**
 * Testing the exponential backoff algorithm.
 */
public class TestExponentialBackoff {

  MockClock mock = new MockClock(0);

  @Test
  public void testBackoff() throws InterruptedException {
    Clock.setClock(mock);
    System.out.println(mock);
    ExponentialBackoff bo = new ExponentialBackoff(100, 5);
    // time = 0
    Assert.assertTrue(bo.isRetryOk());
    Assert.assertFalse(bo.isFailed());

    mock.forward(1); // time = 1
    System.out.println(mock);
    Assert.assertTrue(bo.isRetryOk());
    Assert.assertFalse(bo.isFailed());

    // should be ok in +100, but not at +99
    bo.backoff();
    mock.forward(99);
    System.out.println(mock);
    Assert.assertFalse(bo.isFailed());
    Assert.assertFalse(bo.isRetryOk());

    mock.forward(1); // time = 100
    System.out.println(mock);
    Assert.assertTrue(bo.isRetryOk());
    Assert.assertFalse(bo.isFailed());

    // we were succesfull
    bo.reset();

    // I'm going to cheat and max out backoff.. (note: i should fastforward
    // time.)
    bo.backoff(); // 100
    bo.backoff(); // 200
    bo.backoff(); // 400
    bo.backoff(); // 800
    bo.backoff(); // 1600

    mock.forward(1599);
    System.out.println(mock);
    Assert.assertFalse(bo.isRetryOk());
    Assert.assertTrue(bo.isFailed());

    mock.forward(1);
    System.out.println(mock);
    Assert.assertFalse(bo.isRetryOk()); // still should be false despite time out
    // because in failed state
    Assert.assertTrue(bo.isFailed());

  }
}
