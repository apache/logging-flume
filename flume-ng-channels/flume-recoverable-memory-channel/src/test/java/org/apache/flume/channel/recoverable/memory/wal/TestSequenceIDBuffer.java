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
package org.apache.flume.channel.recoverable.memory.wal;

import java.util.Random;

import org.apache.flume.channel.recoverable.memory.wal.SequenceIDBuffer;
import org.junit.Assert;
import org.junit.Test;

public class TestSequenceIDBuffer {

  @Test
  public void testBinarySearch() {
    int size = 100;
    SequenceIDBuffer buffer = new SequenceIDBuffer(size);
    Assert.assertEquals(size, buffer.size());
    for (int i = 0; i < 100; i++) {
      buffer.put(i, i);
    }
    buffer.sort();
    Assert.assertFalse(buffer.exists(-1));
    Assert.assertFalse(buffer.exists(101));
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(buffer.exists(i));
    }
  }

  @Test
  public void testSortAndCompareTo() {
    int size = 100;
    SequenceIDBuffer buffer = new SequenceIDBuffer(size);
    Assert.assertEquals(size, buffer.size());
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      buffer.put(i, Math.abs(random.nextLong()));
    }

    buffer.sort();

    long last = Long.MIN_VALUE;
    for (int i = 0; i < 100; i++) {
      long current = buffer.get(i);
      Assert.assertTrue(last <= current);
    }
  }

  @Test
  public void testSwap() {
    SequenceIDBuffer buffer = new SequenceIDBuffer(2);
    buffer.put(0, Long.MAX_VALUE);
    buffer.put(1, Long.MIN_VALUE);
    buffer.swap(0, 1);
    Assert.assertEquals(buffer.get(0), Long.MIN_VALUE);
    Assert.assertEquals(buffer.get(1), Long.MAX_VALUE);
  }
}
