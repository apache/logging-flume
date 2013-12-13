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
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.RandomAccessFile;

@RunWith(value = Parameterized.class)
public class TestFlumeEventQueue {
  FlumeEventPointer pointer1 = new FlumeEventPointer(1, 1);
  FlumeEventPointer pointer2 = new FlumeEventPointer(2, 2);
  FlumeEventPointer pointer3 = new FlumeEventPointer(3, 3);
  FlumeEventQueue queue;
  EventQueueBackingStoreSupplier backingStoreSupplier;
  EventQueueBackingStore backingStore;

  private abstract static class EventQueueBackingStoreSupplier {
    File baseDir;
    File checkpoint;
    File inflightTakes;
    File inflightPuts;
    File queueSetDir;
    EventQueueBackingStoreSupplier() {
      baseDir = Files.createTempDir();
      checkpoint = new File(baseDir, "checkpoint");
      inflightTakes = new File(baseDir, "inflightputs");
      inflightPuts =  new File(baseDir, "inflighttakes");
      queueSetDir =  new File(baseDir, "queueset");
    }
    File getCheckpoint() {
      return checkpoint;
    }
    File getInflightPuts() {
      return inflightPuts;
    }
    File getInflightTakes() {
      return inflightTakes;
    }
    File getQueueSetDir() {
      return queueSetDir;
    }
    void delete() {
      FileUtils.deleteQuietly(baseDir);
    }
    abstract EventQueueBackingStore get() throws Exception ;
  }

  @Parameters
  public static Collection<Object[]> data() throws Exception {
    Object[][] data = new Object[][] { {
      new EventQueueBackingStoreSupplier() {
        @Override
        public EventQueueBackingStore get() throws Exception {
          Assert.assertTrue(baseDir.isDirectory() || baseDir.mkdirs());
          return new EventQueueBackingStoreFileV2(getCheckpoint(), 1000,
              "test");
        }
      }
    }, {
      new EventQueueBackingStoreSupplier() {
        @Override
        public EventQueueBackingStore get() throws Exception {
          Assert.assertTrue(baseDir.isDirectory() || baseDir.mkdirs());
          return new EventQueueBackingStoreFileV3(getCheckpoint(), 1000,
              "test");
        }
      }
    } };
    return Arrays.asList(data);
  }

  public TestFlumeEventQueue(EventQueueBackingStoreSupplier backingStoreSupplier) {
    this.backingStoreSupplier = backingStoreSupplier;
  }
  @Before
  public void setup() throws Exception {
    backingStore = backingStoreSupplier.get();
  }
  @After
  public void cleanup() throws IOException {
    if(backingStore != null) {
      backingStore.close();
    }
    backingStoreSupplier.delete();
  }
  @Test
  public void testCapacity() throws Exception {
    backingStore.close();
    File checkpoint = backingStoreSupplier.getCheckpoint();
    Assert.assertTrue(checkpoint.delete());
    backingStore = new EventQueueBackingStoreFileV2(checkpoint, 1, "test");
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertFalse(queue.addTail(pointer2));
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityZero() throws Exception {
    backingStore.close();
    File checkpoint = backingStoreSupplier.getCheckpoint();
    Assert.assertTrue(checkpoint.delete());
    backingStore = new EventQueueBackingStoreFileV2(checkpoint, 0, "test");
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
  }
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidCapacityNegative() throws Exception {
    backingStore.close();
    File checkpoint = backingStoreSupplier.getCheckpoint();
    Assert.assertTrue(checkpoint.delete());
    backingStore = new EventQueueBackingStoreFileV2(checkpoint, -1, "test");
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
  }
  @Test
  public void testQueueIsEmptyAfterCreation() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertNull(queue.removeHead(0L));
  }
  @Test
  public void addTail1() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTail2() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(2), queue.getFileIDs());
  }
  @Test
  public void addTailLarge() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addTail(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = 1; i <= size; i++) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead(0));
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead1() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertEquals(pointer1, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addHead2() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    queue.replayComplete();
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertEquals(Sets.newHashSet(1, 2), queue.getFileIDs());
    Assert.assertEquals(pointer2, queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
  }
  @Test
  public void addHeadLarge() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    queue.replayComplete();
    int size = 500;
    Set<Integer> fileIDs = Sets.newHashSet();
    for (int i = 1; i <= size; i++) {
      Assert.assertTrue(queue.addHead(new FlumeEventPointer(i, i)));
      fileIDs.add(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    for (int i = size; i > 0; i--) {
      Assert.assertEquals(new FlumeEventPointer(i, i), queue.removeHead(0));
      fileIDs.remove(i);
      Assert.assertEquals(fileIDs, queue.getFileIDs());
    }
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }
  @Test
  public void addTailRemove1() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertEquals(Sets.newHashSet(1), queue.getFileIDs());
    Assert.assertTrue(queue.remove(pointer1));
    queue.replayComplete();
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
    Assert.assertNull(queue.removeHead(0));
    Assert.assertEquals(Sets.newHashSet(), queue.getFileIDs());
  }

  @Test
  public void addTailRemove2() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addTail(pointer1));
    Assert.assertTrue(queue.addTail(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    queue.replayComplete();
    Assert.assertEquals(pointer2, queue.removeHead(0));
  }

  @Test
  public void addHeadRemove1() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    queue.addHead(pointer1);
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertNull(queue.removeHead(0));
  }
  @Test
  public void addHeadRemove2() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertTrue(queue.remove(pointer1));
    queue.replayComplete();
    Assert.assertEquals(pointer2, queue.removeHead(0));
  }
  @Test
  public void testUnknownPointerDoesNotCauseSearch() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    Assert.assertTrue(queue.addHead(pointer1));
    Assert.assertTrue(queue.addHead(pointer2));
    Assert.assertFalse(queue.remove(pointer3)); // does search
    Assert.assertTrue(queue.remove(pointer1));
    Assert.assertTrue(queue.remove(pointer2));
    queue.replayComplete();
    Assert.assertEquals(2, queue.getSearchCount());
  }
  @Test(expected=IllegalStateException.class)
  public void testRemoveAfterReplayComplete() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    queue.replayComplete();
    queue.remove(pointer1);
  }
  @Test
  public void testWrappingCorrectly() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    int size = Integer.MAX_VALUE;
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
    for (int i = queue.getSize()/2; i > 0; i--) {
      Assert.assertNotNull(queue.removeHead(0));
    }
    // addHead below would throw an IndexOOBounds with
    // bad version of FlumeEventQueue.convert
    for (int i = 1; i <= size; i++) {
      if(!queue.addHead(new FlumeEventPointer(i, i))) {
        break;
      }
    }
  }
  @Test
  public void testInflightPuts() throws Exception{
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
    long txnID2 = txnID1 + 1;
    queue.addWithoutCommit(new FlumeEventPointer(1, 1), txnID1);
    queue.addWithoutCommit(new FlumeEventPointer(2, 1), txnID1);
    queue.addWithoutCommit(new FlumeEventPointer(2, 2), txnID2);
    queue.checkpoint(true);
    TimeUnit.SECONDS.sleep(3L);
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightPuts();
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(2, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 2).toLong()));
  }

  @Test
  public void testInflightTakes() throws Exception {
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
    long txnID2 = txnID1 + 1;
    queue.addTail(new FlumeEventPointer(1, 1));
    queue.addTail(new FlumeEventPointer(2, 1));
    queue.addTail(new FlumeEventPointer(2, 2));
    queue.removeHead(txnID1);
    queue.removeHead(txnID2);
    queue.removeHead(txnID2);
    queue.checkpoint(true);
    TimeUnit.SECONDS.sleep(3L);
    queue = new FlumeEventQueue(backingStore,
        backingStoreSupplier.getInflightTakes(),
        backingStoreSupplier.getInflightPuts(),
        backingStoreSupplier.getQueueSetDir());
    SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightTakes();
    Assert.assertTrue(deserializedMap.get(
            txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 1).toLong()));
    Assert.assertTrue(deserializedMap.get(
            txnID2).contains(new FlumeEventPointer(2, 2).toLong()));

  }

  @Test(expected = BadCheckpointException.class)
  public void testCorruptInflightPuts() throws Exception {
    RandomAccessFile inflight = null;
    try {
      queue = new FlumeEventQueue(backingStore,
              backingStoreSupplier.getInflightTakes(),
              backingStoreSupplier.getInflightPuts(),
              backingStoreSupplier.getQueueSetDir());
      long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
      long txnID2 = txnID1 + 1;
      queue.addWithoutCommit(new FlumeEventPointer(1, 1), txnID1);
      queue.addWithoutCommit(new FlumeEventPointer(2, 1), txnID1);
      queue.addWithoutCommit(new FlumeEventPointer(2, 2), txnID2);
      queue.checkpoint(true);
      TimeUnit.SECONDS.sleep(3L);
      inflight = new RandomAccessFile(
              backingStoreSupplier.getInflightPuts(), "rw");
      inflight.seek(0);
      inflight.writeInt(new Random().nextInt());
      queue = new FlumeEventQueue(backingStore,
              backingStoreSupplier.getInflightTakes(),
              backingStoreSupplier.getInflightPuts(),
              backingStoreSupplier.getQueueSetDir());
      SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightPuts();
      Assert.assertTrue(deserializedMap.get(
              txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
      Assert.assertTrue(deserializedMap.get(
              txnID1).contains(new FlumeEventPointer(2, 1).toLong()));
      Assert.assertTrue(deserializedMap.get(
              txnID2).contains(new FlumeEventPointer(2, 2).toLong()));
    } finally {
      inflight.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testCorruptInflightTakes() throws Exception {
    RandomAccessFile inflight = null;
    try {
      queue = new FlumeEventQueue(backingStore,
              backingStoreSupplier.getInflightTakes(),
              backingStoreSupplier.getInflightPuts(),
              backingStoreSupplier.getQueueSetDir());
      long txnID1 = new Random().nextInt(Integer.MAX_VALUE - 1);
      long txnID2 = txnID1 + 1;
      queue.addWithoutCommit(new FlumeEventPointer(1, 1), txnID1);
      queue.addWithoutCommit(new FlumeEventPointer(2, 1), txnID1);
      queue.addWithoutCommit(new FlumeEventPointer(2, 2), txnID2);
      queue.checkpoint(true);
      TimeUnit.SECONDS.sleep(3L);
      inflight = new RandomAccessFile(
              backingStoreSupplier.getInflightTakes(), "rw");
      inflight.seek(0);
      inflight.writeInt(new Random().nextInt());
      queue = new FlumeEventQueue(backingStore,
              backingStoreSupplier.getInflightTakes(),
              backingStoreSupplier.getInflightPuts(),
              backingStoreSupplier.getQueueSetDir());
      SetMultimap<Long, Long> deserializedMap = queue.deserializeInflightTakes();
      Assert.assertTrue(deserializedMap.get(
              txnID1).contains(new FlumeEventPointer(1, 1).toLong()));
      Assert.assertTrue(deserializedMap.get(
              txnID1).contains(new FlumeEventPointer(2, 1).toLong()));
      Assert.assertTrue(deserializedMap.get(
              txnID2).contains(new FlumeEventPointer(2, 2).toLong()));
    } finally {
      inflight.close();
    }
  }
}
