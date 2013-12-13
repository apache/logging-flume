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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import org.apache.flume.channel.file.proto.ProtosFactory;

public class TestEventQueueBackingStoreFactory {
  static final List<Long> pointersInTestCheckpoint = Arrays.asList(new Long[] {
      8589936804L,
      4294969563L,
      12884904153L,
      8589936919L,
      4294969678L,
      12884904268L,
      8589937034L,
      4294969793L,
      12884904383L
  });

  File baseDir;
  File checkpoint;
  File inflightTakes;
  File inflightPuts;
  File queueSetDir;
  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    checkpoint = new File(baseDir, "checkpoint");
    inflightTakes = new File(baseDir, "takes");
    inflightPuts = new File(baseDir, "puts");
    queueSetDir = new File(baseDir, "queueset");
    TestUtils.copyDecompressed("fileformat-v2-checkpoint.gz", checkpoint);

  }
  @After
  public void teardown() {
    FileUtils.deleteQuietly(baseDir);
  }
  @Test
  public void testWithNoFlag() throws Exception {
    verify(EventQueueBackingStoreFactory.get(checkpoint, 10, "test"),
        Serialization.VERSION_3, pointersInTestCheckpoint);
  }
  @Test
  public void testWithFlag() throws Exception {
    verify(EventQueueBackingStoreFactory.get(checkpoint, 10, "test", true),
        Serialization.VERSION_3, pointersInTestCheckpoint);
  }
  @Test
  public void testNoUprade() throws Exception {
    verify(EventQueueBackingStoreFactory.get(checkpoint, 10, "test", false),
        Serialization.VERSION_2, pointersInTestCheckpoint);
  }
  @Test (expected = BadCheckpointException.class)
  public void testDecreaseCapacity() throws Exception {
    Assert.assertTrue(checkpoint.delete());
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    EventQueueBackingStoreFactory.get(checkpoint, 9, "test");
    Assert.fail();
  }

  @Test (expected = BadCheckpointException.class)
  public void testIncreaseCapacity() throws Exception {
    Assert.assertTrue(checkpoint.delete());
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    EventQueueBackingStoreFactory.get(checkpoint, 11, "test");
    Assert.fail();
  }

  @Test
  public void testNewCheckpoint() throws Exception {
    Assert.assertTrue(checkpoint.delete());
    verify(EventQueueBackingStoreFactory.get(checkpoint, 10, "test", false),
        Serialization.VERSION_3, Collections.<Long>emptyList());
  }

  @Test (expected = BadCheckpointException.class)
  public void testCheckpointBadVersion() throws Exception {
     RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    try {
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    writer.seek(
            EventQueueBackingStoreFile.INDEX_VERSION * Serialization.SIZE_OF_LONG);
    writer.writeLong(94L);
    writer.getFD().sync();

    backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      writer.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testIncompleteCheckpoint() throws Exception {
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");

    try {
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    writer.seek(
            EventQueueBackingStoreFile.INDEX_CHECKPOINT_MARKER *
            Serialization.SIZE_OF_LONG);
    writer.writeLong(EventQueueBackingStoreFile.CHECKPOINT_INCOMPLETE);
    writer.getFD().sync();
    backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      writer.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testCheckpointVersionNotEqualToMeta() throws Exception {
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    try {
      EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
              get(checkpoint, 10, "test");
      backingStore.close();
      writer.seek(
              EventQueueBackingStoreFile.INDEX_VERSION
              * Serialization.SIZE_OF_LONG);
      writer.writeLong(2L);
      writer.getFD().sync();
      backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      writer.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testCheckpointVersionNotEqualToMeta2() throws Exception {
    FileOutputStream os = null;
    try {
      EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
              get(checkpoint, 10, "test");
      backingStore.close();
      Assert.assertTrue(checkpoint.exists());
      Assert.assertTrue(Serialization.getMetaDataFile(checkpoint).length() != 0);
      FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
      ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
      Assert.assertNotNull(meta);
      is.close();
      os = new FileOutputStream(
              Serialization.getMetaDataFile(checkpoint));
      meta.toBuilder().setVersion(2).build().writeDelimitedTo(os);
      os.flush();
      backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      os.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testCheckpointOrderIdNotEqualToMeta() throws Exception {
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    try {
      EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
              get(checkpoint, 10, "test");
      backingStore.close();
      writer.seek(
              EventQueueBackingStoreFile.INDEX_WRITE_ORDER_ID
              * Serialization.SIZE_OF_LONG);
      writer.writeLong(2L);
      writer.getFD().sync();
      backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      writer.close();
    }
  }

  @Test(expected = BadCheckpointException.class)
  public void testCheckpointOrderIdNotEqualToMeta2() throws Exception {
    FileOutputStream os = null;
    try {
      EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
              get(checkpoint, 10, "test");
      backingStore.close();
      Assert.assertTrue(checkpoint.exists());
      Assert.assertTrue(Serialization.getMetaDataFile(checkpoint).length() != 0);
      FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
      ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
      Assert.assertNotNull(meta);
      is.close();
      os = new FileOutputStream(
              Serialization.getMetaDataFile(checkpoint));
      meta.toBuilder().setWriteOrderID(1).build().writeDelimitedTo(os);
      os.flush();
      backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } finally {
      os.close();
    }
  }


  @Test(expected = BadCheckpointException.class)
  public void testTruncateMeta() throws Exception {
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    Assert.assertTrue(checkpoint.exists());
    File metaFile = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(metaFile.length() != 0);
    RandomAccessFile writer = new RandomAccessFile(metaFile, "rw");
    writer.setLength(0);
    writer.getFD().sync();
    writer.close();
    backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
  }

  @Test (expected = InvalidProtocolBufferException.class)
  public void testCorruptMeta() throws Throwable {
    EventQueueBackingStore backingStore = EventQueueBackingStoreFactory.
            get(checkpoint, 10, "test");
    backingStore.close();
    Assert.assertTrue(checkpoint.exists());
    File metaFile = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(metaFile.length() != 0);
    RandomAccessFile writer = new RandomAccessFile(metaFile, "rw");
    writer.seek(10);
    writer.writeLong(new Random().nextLong());
    writer.getFD().sync();
    writer.close();
    try {
      backingStore = EventQueueBackingStoreFactory.get(checkpoint, 10, "test");
    } catch (BadCheckpointException ex) {
      throw ex.getCause();
    }
  }




  private void verify(EventQueueBackingStore backingStore, long expectedVersion,
      List<Long> expectedPointers)
      throws Exception {
    FlumeEventQueue queue = new FlumeEventQueue(backingStore, inflightTakes,
        inflightPuts, queueSetDir);
    List<Long> actualPointers = Lists.newArrayList();
    FlumeEventPointer ptr;
    while((ptr = queue.removeHead(0L)) != null) {
      actualPointers.add(ptr.toLong());
    }
    Assert.assertEquals(expectedPointers, actualPointers);
    Assert.assertEquals(10, backingStore.getCapacity());
    DataInputStream in = new DataInputStream(new FileInputStream(checkpoint));
    long actualVersion = in.readLong();
    Assert.assertEquals(expectedVersion, actualVersion);
    in.close();
  }
}
