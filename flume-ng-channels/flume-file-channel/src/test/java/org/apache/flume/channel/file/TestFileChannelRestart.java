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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.proto.ProtosFactory;
import org.fest.reflect.exception.ReflectionError;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.apache.flume.channel.file.TestUtils.compareInputAndOut;
import static org.apache.flume.channel.file.TestUtils.consumeChannel;
import static org.apache.flume.channel.file.TestUtils.fillChannel;
import static org.apache.flume.channel.file.TestUtils.forceCheckpoint;
import static org.apache.flume.channel.file.TestUtils.putEvents;
import static org.apache.flume.channel.file.TestUtils.putWithoutCommit;
import static org.apache.flume.channel.file.TestUtils.takeEvents;
import static org.apache.flume.channel.file.TestUtils.takeWithoutCommit;
import static org.fest.reflect.core.Reflection.*;

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
  public void testRestartWhenMetaDataExistsButCheckpointDoesNot() throws
      Exception {
    doTestRestartWhenMetaDataExistsButCheckpointDoesNot(false);
  }

  @Test
  public void testRestartWhenMetaDataExistsButCheckpointDoesNotWithBackup()
      throws Exception {
    doTestRestartWhenMetaDataExistsButCheckpointDoesNot(true);
  }

  private void doTestRestartWhenMetaDataExistsButCheckpointDoesNot(
      boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenCheckpointExistsButMetaDoesNot() throws Exception{
    doTestRestartWhenCheckpointExistsButMetaDoesNot(false);
  }

  @Test
  public void testRestartWhenCheckpointExistsButMetaDoesNotWithBackup() throws
      Exception{
    doTestRestartWhenCheckpointExistsButMetaDoesNot(true);
  }


  private void doTestRestartWhenCheckpointExistsButMetaDoesNot(boolean backup)
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testRestartWhenNoCheckpointExists() throws Exception {
    doTestRestartWhenNoCheckpointExists(false);
  }

  @Test
  public void testRestartWhenNoCheckpointExistsWithBackup() throws Exception {
    doTestRestartWhenNoCheckpointExists(true);
  }

  private void doTestRestartWhenNoCheckpointExists(boolean backup) throws
      Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointVersion() throws Exception {
    doTestBadCheckpointVersion(false);
  }

  @Test
  public void testBadCheckpointVersionWithBackup() throws Exception {
    doTestBadCheckpointVersion(true);
  }

  private void doTestBadCheckpointVersion(boolean backup) throws Exception{
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testBadCheckpointMetaVersion() throws Exception {
    doTestBadCheckpointMetaVersion(false);
  }

  @Test
  public void testBadCheckpointMetaVersionWithBackup() throws Exception {
    doTestBadCheckpointMetaVersion(true);
  }

  private void doTestBadCheckpointMetaVersion(boolean backup) throws
      Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testDifferingOrderIDCheckpointAndMetaVersion() throws Exception {
    doTestDifferingOrderIDCheckpointAndMetaVersion(false);
  }

  @Test
  public void testDifferingOrderIDCheckpointAndMetaVersionWithBackup() throws
      Exception {
    doTestDifferingOrderIDCheckpointAndMetaVersion(true);
  }

  private void doTestDifferingOrderIDCheckpointAndMetaVersion(boolean backup)
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testIncompleteCheckpoint() throws Exception{
    doTestIncompleteCheckpoint(false);
  }

  @Test
  public void testIncompleteCheckpointWithCheckpoint() throws Exception{
    doTestIncompleteCheckpoint(true);
  }

  private void doTestIncompleteCheckpoint(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptInflightPuts() throws Exception {
    doTestCorruptInflights("inflightputs", false);
  }

  @Test
  public void testCorruptInflightPutsWithBackup() throws Exception {
    doTestCorruptInflights("inflightputs", true);
  }

  @Test
  public void testCorruptInflightTakes() throws Exception {
    doTestCorruptInflights("inflighttakes", false);
  }

  @Test
  public void testCorruptInflightTakesWithBackup() throws Exception {
    doTestCorruptInflights("inflighttakes", true);
  }

  @Test
  public void testFastReplayWithCheckpoint() throws Exception{
    testFastReplay(false, true);
  }

  @Test
  public void testFastReplayWithBadCheckpoint() throws Exception{
    testFastReplay(true, true);
  }

  @Test
  public void testNoFastReplayWithCheckpoint() throws Exception{
    testFastReplay(false, false);
  }

  @Test
  public void testNoFastReplayWithBadCheckpoint() throws Exception{
    testFastReplay(true, false);
  }

  private void testFastReplay(boolean shouldCorruptCheckpoint,
                             boolean useFastReplay) throws Exception{
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_FAST_REPLAY,
      String.valueOf(useFastReplay));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    channel.stop();
    if (shouldCorruptCheckpoint) {
      File checkpoint = new File(checkpointDir, "checkpoint");
      RandomAccessFile writer = new RandomAccessFile(
        Serialization.getMetaDataFile(checkpoint), "rw");
      writer.seek(10);
      writer.writeLong(new Random().nextLong());
      writer.getFD().sync();
      writer.close();
    }
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    if (useFastReplay && shouldCorruptCheckpoint) {
      Assert.assertTrue(channel.didFastReplay());
    } else {
      Assert.assertFalse(channel.didFastReplay());
    }
    compareInputAndOut(in, out);
  }

  private void doTestCorruptInflights(String name,
    boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    final Set<String> in1 = putEvents(channel, "restart-",10, 100);
    Assert.assertEquals(100, in1.size());
    Executors.newSingleThreadScheduledExecutor().submit(new Runnable() {
      @Override
      public void run() {
        Transaction tx = channel.getTransaction();
        Set<String> out1 = takeWithoutCommit(channel, tx, 100);
        Assert.assertEquals(100, out1.size());
      }
    });
    Transaction tx = channel.getTransaction();
    Set<String> in2 = putWithoutCommit(channel, tx, "restart", 100);
    Assert.assertEquals(100, in2.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
    tx.commit();
    tx.close();
    channel.stop();
    File inflight = new File(checkpointDir, name);
    RandomAccessFile writer = new RandomAccessFile(inflight, "rw");
    writer.write(new Random().nextInt());
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    in1.addAll(in2);
    compareInputAndOut(in1, out);
  }

  @Test
  public void testTruncatedCheckpointMeta() throws Exception {
    doTestTruncatedCheckpointMeta(false);
  }

  @Test
  public void testTruncatedCheckpointMetaWithBackup() throws Exception {
    doTestTruncatedCheckpointMeta(true);
  }

  private void doTestTruncatedCheckpointMeta(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  @Test
  public void testCorruptCheckpointMeta() throws Exception {
    doTestCorruptCheckpointMeta(false);
  }

  @Test
  public void testCorruptCheckpointMetaWithBackup() throws Exception {
    doTestCorruptCheckpointMeta(true);
  }

  private void doTestCorruptCheckpointMeta(boolean backup) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, String.valueOf(backup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    forceCheckpoint(channel);
    if(backup) {
      Thread.sleep(2000);
    }
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
    Assert.assertTrue(!backup || channel.checkpointBackupRestored());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }

  private void checkIfBackupUsed(boolean backup) {
    boolean backupRestored = channel.checkpointBackupRestored();
    if (backup) {
      Assert.assertTrue(backupRestored);
    } else {
      Assert.assertFalse(backupRestored);
    }
  }
 
  //This test will fail without FLUME-1893
  @Test
  public void testCorruptCheckpointVersionMostSignificant4Bytes()
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
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_VERSION *
      Serialization.SIZE_OF_LONG);
    writer.write(new byte[]{(byte)1, (byte)5});
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    Assert.assertTrue(channel.didFullReplayDueToBadCheckpointException());
    compareInputAndOut(in, out);
  }

  //This test will fail without FLUME-1893
  @Test
  public void testCorruptCheckpointCompleteMarkerMostSignificant4Bytes()
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
    RandomAccessFile writer = new RandomAccessFile(checkpoint, "rw");
    writer.seek(EventQueueBackingStoreFile.INDEX_CHECKPOINT_MARKER *
      Serialization.SIZE_OF_LONG);
    writer.write(new byte[]{(byte) 1, (byte) 5});
    writer.getFD().sync();
    writer.close();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    Assert.assertTrue(channel.didFullReplayDueToBadCheckpointException());
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

  // Make sure the entire channel was not replayed, only the events from the
  // backup.
  @Test
  public void testBackupUsedEnsureNoFullReplayWithoutCompression() throws
    Exception {
    testBackupUsedEnsureNoFullReplay(false);
  }
  @Test
  public void testBackupUsedEnsureNoFullReplayWithCompression() throws
    Exception {
    testBackupUsedEnsureNoFullReplay(true);
  }

  private void testBackupUsedEnsureNoFullReplay(boolean compressedBackup)
    throws Exception {
    File dataDir = Files.createTempDir();
    File tempBackup = Files.createTempDir();
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.DATA_DIRS,
      dataDir.getAbsolutePath());
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS,
      "true");
    overrides.put(FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
      String.valueOf(compressedBackup));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    Thread.sleep(5000);
    forceCheckpoint(channel);
    Thread.sleep(5000);
    in = putEvents(channel, "restart", 10, 100);
    takeEvents(channel, 10, 100);
    Assert.assertEquals(100, in.size());
    for(File file : backupDir.listFiles()) {
      if(file.getName().equals(Log.FILE_LOCK)) {
        continue;
      }
      Files.copy(file, new File(tempBackup, file.getName()));
    }
    forceCheckpoint(channel);
    channel.stop();

    Serialization.deleteAllFiles(checkpointDir, Log.EXCLUDES);
    // The last checkpoint may have been already backed up (it did while I
    // was running this test, since the checkpoint itself is tiny in unit
    // tests), so throw away the backup and force the use of an older backup by
    // bringing in the copy of the last backup before the checkpoint.
    Serialization.deleteAllFiles(backupDir, Log.EXCLUDES);
    for(File file : tempBackup.listFiles()) {
      if(file.getName().equals(Log.FILE_LOCK)) {
        continue;
      }
      Files.copy(file, new File(backupDir, file.getName()));
    }
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    checkIfBackupUsed(true);
    Assert.assertEquals(100, channel.getLog().getPutCount());
    Assert.assertEquals(20, channel.getLog().getCommittedCount());
    Assert.assertEquals(100, channel.getLog().getTakeCount());
    Assert.assertEquals(0, channel.getLog().getRollbackCount());
    //Read Count = 100 puts + 10 commits + 100 takes + 10 commits
    Assert.assertEquals(220, channel.getLog().getReadCount());
    consumeChannel(channel);
    FileUtils.deleteQuietly(dataDir);
    FileUtils.deleteQuietly(tempBackup);
  }

  //Make sure data files required by the backup checkpoint are not deleted.
  @Test
  public void testDataFilesRequiredByBackupNotDeleted() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, "true");
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "1000");
    channel = createFileChannel(overrides);
    channel.start();
    String prefix = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    Assert.assertTrue(channel.isOpen());
    putEvents(channel, prefix, 10, 100);
    Set<String> origFiles = Sets.newHashSet();
    for(File dir : dataDirs) {
      origFiles.addAll(Lists.newArrayList(dir.list()));
    }
    forceCheckpoint(channel);
    takeEvents(channel, 10, 50);
    long beforeSecondCheckpoint = System.currentTimeMillis();
    forceCheckpoint(channel);
    Set<String> newFiles = Sets.newHashSet();
    int olderThanCheckpoint = 0;
    int totalMetaFiles = 0;
    for(File dir : dataDirs) {
      File[] metadataFiles = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.endsWith(".meta")) {
            return true;
          }
          return false;
        }
      });
      totalMetaFiles = metadataFiles.length;
      for(File metadataFile : metadataFiles) {
        if(metadataFile.lastModified() < beforeSecondCheckpoint) {
          olderThanCheckpoint++;
        }
      }
      newFiles.addAll(Lists.newArrayList(dir.list()));
    }
    /*
     * Files which are not required by the new checkpoint should not have been
     * modified by the checkpoint.
     */
    Assert.assertTrue(olderThanCheckpoint > 0);
    Assert.assertTrue(totalMetaFiles != olderThanCheckpoint);

    /*
     * All files needed by original checkpoint should still be there.
     */
    Assert.assertTrue(newFiles.containsAll(origFiles));
    takeEvents(channel, 10, 50);
    forceCheckpoint(channel);
    newFiles = Sets.newHashSet();
    for(File dir : dataDirs) {
      newFiles.addAll(Lists.newArrayList(dir.list()));
    }
    Assert.assertTrue(!newFiles.containsAll(origFiles));
  }

  @Test (expected = IOException.class)
  public void testSlowBackup() throws Throwable {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, "true");
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "1000");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = putEvents(channel, "restart", 10, 100);
    Assert.assertEquals(100, in.size());
    slowdownBackup(channel);
    forceCheckpoint(channel);
    in = putEvents(channel, "restart", 10, 100);
    takeEvents(channel, 10, 100);
    Assert.assertEquals(100, in.size());
    try {
      forceCheckpoint(channel);
    } catch (ReflectionError ex) {
      throw ex.getCause();
    } finally {
      channel.stop();
    }
  }

  @Test
  public void testCompressBackup() throws Throwable {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS,
      "true");
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "1000");
    overrides.put(FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
      "true");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    putEvents(channel, "restart", 10, 100);
    forceCheckpoint(channel);

    //Wait for the backup checkpoint
    Thread.sleep(2000);

    Assert.assertTrue(compressedBackupCheckpoint.exists());

    Serialization.decompressFile(compressedBackupCheckpoint,
      uncompressedBackupCheckpoint);

    File checkpoint = new File(checkpointDir, "checkpoint");
    Assert.assertTrue(FileUtils.contentEquals(checkpoint,
      uncompressedBackupCheckpoint));

    channel.stop();
  }

  @Test
  public void testToggleCheckpointCompressionFromTrueToFalse()
    throws Exception {
    restartToggleCompression(true);
  }

  @Test
  public void testToggleCheckpointCompressionFromFalseToTrue()
    throws Exception {
    restartToggleCompression(false);
  }

  public void restartToggleCompression(boolean originalCheckpointCompressed)
    throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS,
      "true");
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "1000");
    overrides.put(FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
      String.valueOf(originalCheckpointCompressed));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    forceCheckpoint(channel);
    Thread.sleep(2000);
    Assert.assertEquals(compressedBackupCheckpoint.exists(),
      originalCheckpointCompressed);
    Assert.assertEquals(uncompressedBackupCheckpoint.exists(),
      !originalCheckpointCompressed);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    Assert.assertTrue(checkpoint.delete());
    File checkpointMetaData = Serialization.getMetaDataFile(
      checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    overrides.put(FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
      String.valueOf(!originalCheckpointCompressed));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
    forceCheckpoint(channel);
    Thread.sleep(2000);
    Assert.assertEquals(compressedBackupCheckpoint.exists(),
      !originalCheckpointCompressed);
    Assert.assertEquals(uncompressedBackupCheckpoint.exists(),
      originalCheckpointCompressed);
  }

  private static void slowdownBackup(FileChannel channel) {
    Log log = field("log").ofType(Log.class).in(channel).get();

    FlumeEventQueue queue = field("queue")
      .ofType(FlumeEventQueue.class)
      .in(log).get();

    EventQueueBackingStore backingStore = field("backingStore")
      .ofType(EventQueueBackingStore.class)
      .in(queue).get();

    field("slowdownBackup").ofType(Boolean.class).in(backingStore).set(true);
  }
}
