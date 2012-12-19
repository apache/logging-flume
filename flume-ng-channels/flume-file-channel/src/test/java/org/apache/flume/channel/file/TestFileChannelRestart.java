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

import static org.apache.flume.channel.file.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import org.apache.flume.channel.file.proto.ProtosFactory;

public class TestFileChannelRestart extends TestFileChannelBase {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannelRestart.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
  }
  @Test
  public void testRestartLogReplayV1() throws Exception {
    doTestRestart(true, false, false, false);
  }
  @Test
  public void testRestartLogReplayV2() throws Exception {
    doTestRestart(false, false, false, false);
  }

  @Test
  public void testFastReplayV1() throws Exception {
    doTestRestart(true, true, true, true);
  }

  @Test
  public void testFastReplayV2() throws Exception {
    doTestRestart(false, true, true, true);
  }

  @Test
  public void testFastReplayNegativeTestV1() throws Exception {
    doTestRestart(true, true, false, true);
  }

  @Test
  public void testFastReplayNegativeTestV2() throws Exception {
    doTestRestart(false, true, false, true);
  }

  @Test
  public void testNormalReplayV1() throws Exception {
    doTestRestart(true, true, true, false);
  }

  @Test
  public void testNormalReplayV2() throws Exception {
    doTestRestart(false, true, true, false);
  }

  public void doTestRestart(boolean useLogReplayV1,
          boolean forceCheckpoint, boolean deleteCheckpoint,
          boolean useFastReplay) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_LOG_REPLAY_V1,
            String.valueOf(useLogReplayV1));
    overrides.put(
            FileChannelConfiguration.USE_FAST_REPLAY,
            String.valueOf(useFastReplay));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    if (forceCheckpoint) {
      forceCheckpoint(channel);
    }
    channel.stop();
    if(deleteCheckpoint) {
      File checkpoint = new File(checkpointDir, "checkpoint");
      Assert.assertTrue(checkpoint.delete());
      File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
      Assert.assertTrue(checkpointMetaData.delete());
    }
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testRestartWhenMetaDataExistsButCheckpointDoesNot()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    Assert.assertTrue(checkpoint.delete());
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testRestartWhenCheckpointExistsButMetaDoesNot()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    Assert.assertTrue(checkpoint.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenNoCheckpointExists() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    Assert.assertTrue(checkpoint.delete());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(checkpoint.exists());
    Assert.assertTrue(checkpointMetaData.exists());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointVersion() throws Exception{
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_VERSION *
            Serialization.SIZE_OF_LONG);
    writer.writeLong(2L);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointMetaVersion() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
    ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
    Assert.assertNotNull(meta);
    is.close();
    FileOutputStream os = new FileOutputStream(
            Serialization.getMetaDataFile(checkpoint));
    meta.toBuilder().setVersion(2).build().writeDelimitedTo(os);
    os.flush();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testDifferingOrderIDCheckpointAndMetaVersion() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    FileInputStream is = new FileInputStream(Serialization.getMetaDataFile(checkpoint));
    ProtosFactory.Checkpoint meta = ProtosFactory.Checkpoint.parseDelimitedFrom(is);
    Assert.assertNotNull(meta);
    is.close();
    FileOutputStream os = new FileOutputStream(
            Serialization.getMetaDataFile(checkpoint));
    meta.toBuilder().setWriteOrderID(12).build().writeDelimitedTo(os);
    os.flush();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testIncompleteCheckpoint() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_CHECKPOINT_MARKER
            * Serialization.SIZE_OF_LONG);
    writer.writeLong(EventQueueBackingStoreFile.CHECKPOINT_INCOMPLETE);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptInflightPuts() throws Exception {
    testCorruptInflights("inflightPuts");
  }

  @Test
  public void testCorruptInflightTakes() throws Exception {
    testCorruptInflights("inflightTakes");
  }

  private void testCorruptInflights(String name) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File inflight = new File(checkpointDir, name);
    RandomAccessFile writer = new RandomAccessFile(inflight, "rw");
    writer.write(new Random().nextInt());
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testTruncatedCheckpointMeta() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(
            Serialization.getMetaDataFile(checkpoint), "rw");
    writer.setLength(0);
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptCheckpointMeta() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    RandomAccessFile writer = new RandomAccessFile(
            Serialization.getMetaDataFile(checkpoint), "rw");
    writer.seek(10);
    writer.writeLong(new Random().nextLong());
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }


  @Test
  public void testWithExtraLogs()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, "10");
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "extralogs");
    for (int i = 0; i < dataDirs.length; i++) {
      File file = new File(dataDirs[i], Log.PREFIX + (1000 + i));
      Assert.assertTrue(file.createNewFile());
      Assert.assertTrue(file.length() == 0);
      File metaDataFile = Serialization.getMetaDataFile(file);
      File metaDataTempFile = Serialization.getMetaDataTempFile(metaDataFile);
      Assert.assertTrue(metaDataTempFile.createNewFile());
    }
    channel.stop();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
}
