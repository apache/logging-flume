/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume;

import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.flume.CounterGroup;
import org.junit.Before;
import org.junit.Test;

public class TestCounterGroup {

  private CounterGroup counterGroup;

  @Before
  public void setUp() {
    counterGroup = new CounterGroup();
  }

  @Test
  public void testGetCounter() {
    AtomicLong counter = counterGroup.getCounter("test");

    Assert.assertNotNull(counter);
    Assert.assertEquals(0, counter.get());
  }

  @Test
  public void testGet() {
    long value = counterGroup.get("test");

    Assert.assertEquals(0, value);
  }

  @Test
  public void testIncrementAndGet() {
    long value = counterGroup.incrementAndGet("test");

    Assert.assertEquals(1, value);
  }

  @Test
  public void testAddAndGet() {
    long value = counterGroup.addAndGet("test", 13L);

    Assert.assertEquals(13, value);
  }

}
