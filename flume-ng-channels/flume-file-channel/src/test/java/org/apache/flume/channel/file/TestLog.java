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

import static org.mockito.Mockito.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.*;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestLog {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestLog.class);
  private static final long MAX_FILE_SIZE = 1000;
  private static final int CAPACITY = 10000;
  private Log log;
  private File checkpointDir;
  private File[] dataDirs;
  private long transactionID;
  @Before
  public void setup() throws IOException {
    transactionID = 0;
    checkpointDir = Files.createTempDir();
    FileUtils.forceDeleteOnExit(checkpointDir);
    Assert.assertTrue(checkpointDir.isDirectory());
    dataDirs = new File[3];
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = Files.createTempDir();
      Assert.assertTrue(dataDirs[i].isDirectory());
    }
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
        MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs).setCheckpointOnClose(false)
            .setChannelName("testlog").build();
    log.replay();
  }
  @After
  public void cleanup() throws Exception{
    if(log != null) {
      log.close();
    }
    FileUtils.deleteQuietly(checkpointDir);
    for (int i = 0; i < dataDirs.length; i++) {
      FileUtils.deleteQuietly(dataDirs[i]);
    }
  }
  /**
   * Test that we can put, commit and then get. Note that get is
   * not transactional so the commit is not required.
   */
  @Test
  public void testPutGet()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    FlumeEventPointer eventPointer = log.put(transactionID, eventIn);
    log.commitPut(transactionID); // this is not required since
    // get is not transactional
    FlumeEvent eventOut = log.get(eventPointer);
    Assert.assertNotNull(eventOut);
    Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
    Assert.assertArrayEquals(eventIn.getBody(), eventOut.getBody());
  }
  @Test
  public void testRoll()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    log.shutdownWorker();
    Thread.sleep(1000);
    for (int i = 0; i < 1000; i++) {
      FlumeEvent eventIn = TestUtils.newPersistableEvent();
      long transactionID = ++this.transactionID;
      FlumeEventPointer eventPointer = log.put(transactionID, eventIn);
      // get is not transactional
      FlumeEvent eventOut = log.get(eventPointer);
      Assert.assertNotNull(eventOut);
      Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
      Assert.assertArrayEquals(eventIn.getBody(), eventOut.getBody());
    }
    int logCount = 0;
    for(File dataDir : dataDirs) {
      for(File logFile : dataDir.listFiles()) {
        if(logFile.getName().startsWith("log-")) {
          logCount++;
        }
      }
    }
    // 93 (*2 for meta) files with TestLog.MAX_FILE_SIZE=1000
    Assert.assertEquals(186, logCount);
  }
  /**
   * After replay of the log, we should find the event because the put
   * was committed
   */
  @Test
  public void testPutCommit()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    FlumeEventPointer eventPointerIn = log.put(transactionID, eventIn);
    log.commitPut(transactionID);
    log.close();
    log = new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            CAPACITY).setCheckpointDir(checkpointDir).setLogDirs(
                dataDirs).setChannelName("testlog").build();
    log.replay();
    takeAndVerify(eventPointerIn, eventIn);
  }
  /**
   * After replay of the log, we should not find the event because the
   * put was rolled back
   */
  @Test
  public void testPutRollback() throws IOException, InterruptedException {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    log.put(transactionID, eventIn);
    log.rollback(transactionID); // rolled back so it should not be replayed
    log.close();
    log = new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            CAPACITY).setCheckpointDir(checkpointDir).setLogDirs(
                dataDirs).setChannelName("testlog").build();
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Assert.assertNull(queue.removeHead(transactionID));
  }
  @Test
  public void testMinimumRequiredSpaceTooSmallOnStartup() throws IOException,
    InterruptedException {
    log.close();
    log = new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            CAPACITY).setCheckpointDir(checkpointDir).setLogDirs(
                dataDirs).setChannelName("testlog").
                setMinimumRequiredSpace(Long.MAX_VALUE).build();
    try {
      log.replay();
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage()
          .startsWith("Usable space exhausted"));
    }
  }
  /**
   * There is a race here in that someone could take up some space
   */
  @Test
  public void testMinimumRequiredSpaceTooSmallForPut() throws IOException,
    InterruptedException {
    try {
      doTestMinimumRequiredSpaceTooSmallForPut();
    } catch (IOException e) {
      LOGGER.info("Error during test, retrying", e);
      doTestMinimumRequiredSpaceTooSmallForPut();
    } catch (AssertionError e) {
      LOGGER.info("Test failed, let's be sure it failed for good reason", e);
      doTestMinimumRequiredSpaceTooSmallForPut();
    }
  }
  public void doTestMinimumRequiredSpaceTooSmallForPut() throws IOException,
    InterruptedException {
    long minimumRequiredSpace = checkpointDir.getUsableSpace() -
        (10L* 1024L * 1024L);
    log.close();
    log = new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            CAPACITY).setCheckpointDir(checkpointDir).setLogDirs(
                dataDirs).setChannelName("testlog").
                setMinimumRequiredSpace(minimumRequiredSpace)
                .setUsableSpaceRefreshInterval(1L).build();
    log.replay();
    File filler = new File(checkpointDir, "filler");
    byte[] buffer = new byte[64 * 1024];
    FileOutputStream out = new FileOutputStream(filler);
    while(checkpointDir.getUsableSpace() > minimumRequiredSpace) {
      out.write(buffer);
    }
    out.close();
    try {
      FlumeEvent eventIn = TestUtils.newPersistableEvent();
      long transactionID = ++this.transactionID;
      log.put(transactionID, eventIn);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage()
          .startsWith("Usable space exhausted"));
    }
  }
  /**
   * After replay of the log, we should not find the event because the take
   * was committed
   */
  @Test
  public void testPutTakeCommit() throws IOException, InterruptedException {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long putTransactionID = ++transactionID;
    FlumeEventPointer eventPointer = log.put(putTransactionID, eventIn);
    log.commitPut(putTransactionID);
    long takeTransactionID = ++transactionID;
    log.take(takeTransactionID, eventPointer);
    log.commitTake(takeTransactionID);
    log.close();
    new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            1).setCheckpointDir(checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").build();
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Assert.assertNull(queue.removeHead(0));
  }

  /**
   * After replay of the log, we should get the event because the take
   * was rolled back
   */
  @Test
  public void testPutTakeRollbackLogReplayV1()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    doPutTakeRollback(true);
  }
  @Test
  public void testPutTakeRollbackLogReplayV2()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    doPutTakeRollback(false);
  }
  public void doPutTakeRollback(boolean useLogReplayV1)
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long putTransactionID = ++transactionID;
    FlumeEventPointer eventPointerIn = log.put(putTransactionID, eventIn);
    log.commitPut(putTransactionID);
    long takeTransactionID = ++transactionID;
    log.take(takeTransactionID, eventPointerIn);
    log.rollback(takeTransactionID);
    log.close();
    new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            1).setCheckpointDir(checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").setUseLogReplayV1(useLogReplayV1).build();
    log.replay();
    takeAndVerify(eventPointerIn, eventIn);
  }

  @Test
  public void testCommitNoPut() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.commitPut(putTransactionID);
    log.close();
    new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            1).setCheckpointDir(checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").build();
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead(0);
    Assert.assertNull(eventPointerOut);
  }

  @Test
  public void testCommitNoTake() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.commitTake(putTransactionID);
    log.close();
    new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            1).setCheckpointDir(checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").build();
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead(0);
    Assert.assertNull(eventPointerOut);
  }

  @Test
  public void testRollbackNoPutTake() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.rollback(putTransactionID);
    log.close();
    new Log.Builder().setCheckpointInterval(
        Long.MAX_VALUE).setMaxFileSize(
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE).setQueueSize(
            1).setCheckpointDir(checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").build();
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead(0);
    Assert.assertNull(eventPointerOut);
  }

  @Test
  public void testGetLogs() throws IOException {
    File logDir = dataDirs[0];
    List<File> expected = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      File log = new File(logDir, Log.PREFIX + i);
      expected.add(log);
      Assert.assertTrue(log.isFile() || log.createNewFile());
      File metaDataFile = Serialization.getMetaDataFile(log);
      File metaDataTempFile = Serialization.getMetaDataTempFile(metaDataFile);
      File logGzip = new File(logDir, Log.PREFIX + i + ".gz");
      Assert.assertTrue(metaDataFile.isFile() || metaDataFile.createNewFile());
      Assert.assertTrue(metaDataTempFile.isFile() ||
          metaDataTempFile.createNewFile());
      Assert.assertTrue(log.isFile() || logGzip.createNewFile());
    }
    List<File> actual = LogUtils.getLogs(logDir);
    LogUtils.sort(actual);
    LogUtils.sort(expected);
    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testReplayFailsWithAllEmptyLogMetaDataNormalReplay()
      throws IOException, InterruptedException {
    doTestReplayFailsWithAllEmptyLogMetaData(false);
  }
  @Test
  public void testReplayFailsWithAllEmptyLogMetaDataFastReplay()
      throws IOException, InterruptedException {
    doTestReplayFailsWithAllEmptyLogMetaData(true);
  }
  public void doTestReplayFailsWithAllEmptyLogMetaData(boolean useFastReplay)
      throws IOException, InterruptedException {
    // setup log with correct fast replay parameter
    log.close();
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
        MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").setUseFastReplay(useFastReplay).build();
    log.replay();
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    log.put(transactionID, eventIn);
    log.commitPut(transactionID);
    log.close();
    if(useFastReplay) {
      FileUtils.deleteQuietly(checkpointDir);
      Assert.assertTrue(checkpointDir.mkdir());
    }
    List<File> logFiles = Lists.newArrayList();
    for (int i = 0; i < dataDirs.length; i++) {
      logFiles.addAll(LogUtils.getLogs(dataDirs[i]));
    }
    Assert.assertTrue(logFiles.size() > 0);
    for(File logFile : logFiles) {
      File logFileMeta = Serialization.getMetaDataFile(logFile);
      Assert.assertTrue(logFileMeta.delete());
      Assert.assertTrue(logFileMeta.createNewFile());
    }
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
        MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").setUseFastReplay(useFastReplay).build();
    try {
      log.replay();
      Assert.fail();
    } catch(IllegalStateException expected) {
      String msg = expected.getMessage();
      Assert.assertNotNull(msg);
      Assert.assertTrue(msg, msg.contains(".meta is empty, but log"));
    }
  }
  @Test
  public void testReplaySucceedsWithUnusedEmptyLogMetaDataNormalReplay()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    FlumeEventPointer eventPointer = log.put(transactionID, eventIn);
    log.commitPut(transactionID); // this is not required since
    log.close();
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
        MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").build();
    doTestReplaySucceedsWithUnusedEmptyLogMetaData(eventIn, eventPointer);
  }
  @Test
  public void testReplaySucceedsWithUnusedEmptyLogMetaDataFastReplay()
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    FlumeEventPointer eventPointer = log.put(transactionID, eventIn);
    log.commitPut(transactionID); // this is not required since
    log.close();
    checkpointDir = Files.createTempDir();
    FileUtils.forceDeleteOnExit(checkpointDir);
    Assert.assertTrue(checkpointDir.isDirectory());
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
        MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs)
            .setChannelName("testlog").setUseFastReplay(true).build();
    doTestReplaySucceedsWithUnusedEmptyLogMetaData(eventIn, eventPointer);
  }
  public void doTestReplaySucceedsWithUnusedEmptyLogMetaData(FlumeEvent eventIn,
      FlumeEventPointer eventPointer) throws IOException,
    InterruptedException, NoopRecordException, CorruptEventException  {
    for (int i = 0; i < dataDirs.length; i++) {
      for(File logFile : LogUtils.getLogs(dataDirs[i])) {
        if(logFile.length() == 0L) {
          File logFileMeta = Serialization.getMetaDataFile(logFile);
          Assert.assertTrue(logFileMeta.delete());
          Assert.assertTrue(logFileMeta.createNewFile());
        }
      }
    }
    log.replay();
    FlumeEvent eventOut = log.get(eventPointer);
    Assert.assertNotNull(eventOut);
    Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
    Assert.assertArrayEquals(eventIn.getBody(), eventOut.getBody());
  }
  @Test
  public void testCachedFSUsableSpace() throws Exception {
    File fs = mock(File.class);
    when(fs.getUsableSpace()).thenReturn(Long.MAX_VALUE);
    LogFile.CachedFSUsableSpace cachedFS =
        new LogFile.CachedFSUsableSpace(fs, 1000L);
    Assert.assertEquals(cachedFS.getUsableSpace(), Long.MAX_VALUE);
    cachedFS.decrement(Integer.MAX_VALUE);
    Assert.assertEquals(cachedFS.getUsableSpace(),
        Long.MAX_VALUE - Integer.MAX_VALUE);
    try {
      cachedFS.decrement(-1);
      Assert.fail();
    } catch (IllegalArgumentException expected) {

    }
    when(fs.getUsableSpace()).thenReturn(Long.MAX_VALUE - 1L);
    Thread.sleep(1100);
    Assert.assertEquals(cachedFS.getUsableSpace(),
        Long.MAX_VALUE - 1L);
  }

  @Test
  public void testCheckpointOnClose() throws Exception {
    log.close();
    log = new Log.Builder().setCheckpointInterval(1L).setMaxFileSize(
            MAX_FILE_SIZE).setQueueSize(CAPACITY).setCheckpointDir(
            checkpointDir).setLogDirs(dataDirs).setCheckpointOnClose(true)
            .setChannelName("testLog").build();
    log.replay();


    // 1 Write One Event
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    log.put(transactionID, eventIn);
    log.commitPut(transactionID);

    // 2 Check state of checkpoint before close
    File checkPointMetaFile =
            FileUtils.listFiles(checkpointDir,new String[]{"meta"},false).iterator().next();
    long before = FileUtils.checksumCRC32( checkPointMetaFile );

    // 3 Close Log
    log.close();

    // 4 Verify that checkpoint was modified on close
    long after = FileUtils.checksumCRC32( checkPointMetaFile );
    Assert.assertFalse( before == after );
  }

  private void takeAndVerify(FlumeEventPointer eventPointerIn,
      FlumeEvent eventIn)
    throws IOException, InterruptedException, NoopRecordException, CorruptEventException  {
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead(0);
    Assert.assertNotNull(eventPointerOut);
    Assert.assertNull(queue.removeHead(0));
    Assert.assertEquals(eventPointerIn, eventPointerOut);
    Assert.assertEquals(eventPointerIn.hashCode(), eventPointerOut.hashCode());
    FlumeEvent eventOut = log.get(eventPointerOut);
    Assert.assertNotNull(eventOut);
    Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
    Assert.assertArrayEquals(eventIn.getBody(), eventOut.getBody());
  }

}
