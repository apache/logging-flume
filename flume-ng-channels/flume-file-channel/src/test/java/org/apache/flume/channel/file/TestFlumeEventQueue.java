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
package org.apache.flume.channel.file;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestFlumeEventQueue {

  File file;
  FlumeEventPointer pointer1 = new FlumeEventPointer(1, 1);
  FlumeEventPointer pointer2 = new FlumeEventPointer(2, 2);
  FlumeEventQueue queue;
  @Before
  public void setup() throws Exception {
    file = File.createTempFile("Checkpoint", "");
  }
  @Test
  public void testQueueIsEmptyAfterCreation() throws IOException {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertNull(queue.removeHead());
  }
  @Test
  public void testCapacity() throws Exception {
    queue = new FlumeEventQueue(1, file, "test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertFalse(queue.addTail(pointer2));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityZero() throws Exception {
    queue = new FlumeEventQueue(0, file, "test");
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityNegative() throws Exception {
    queue = new FlumeEventQueue(-1, file, "test");
  }
  @Test
  public void addTail1() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(pointer1, queue.removeHead());
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTail2() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead());
    Assert.assertEquals(Sets.newHashSet(2), queue.getFileIDs());
  }
  @Test
  public void addTailLarge() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addTail(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = 1; i <= size; i++) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead());
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead1() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead());
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead2() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer2, queue.removeHead());
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
  }
  @Test
  public void addHeadLarge() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addHead(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = size; i > 0; i--) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead());
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTailRemove1() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
    Assert.assertNull(queue.removeHead());
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }

  @Test
  public void addTailRemove2() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(pointer2, queue.removeHead());
  }

  @Test
  public void addHeadRemove1() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    queue.addHead(pointer1);
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertNull(queue.removeHead());
  }
  @Test
  public void addHeadRemove2() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertEquals(pointer2, queue.removeHead());
  }
  @Test
  public void testWrappingCorrectly() throws Exception {
    queue = new FlumeEventQueue(1000, file, "test");
    int size = Integer.MAX_VALUE;
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
    for (int i = queue.getSize()/2; i > 0; i--) {
      Assert.assertNotNull(queue.removeHead());
    }
    // addHead below would throw an IndexOOBounds with
    // bad version of FlumeEventQueue.convert
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
  }
}
