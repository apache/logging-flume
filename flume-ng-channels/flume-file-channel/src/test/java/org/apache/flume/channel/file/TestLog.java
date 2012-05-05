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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestLog {
  private static final long MAX_FILE_SIZE = 1000;
  private Log log;
  private File checkpointDir;
  private File[] dataDirs;
  private long transactionID;
  @Before
  public void setup() throws IOException {
    checkpointDir = Files.createTempDir();
    Assert.assertTrue(checkpointDir.isDirectory());
    dataDirs = new File[3];
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = Files.createTempDir();
      Assert.assertTrue(dataDirs[i].isDirectory());
    }
    log = new Log(1L, MAX_FILE_SIZE, 10000,
        checkpointDir, dataDirs);
    log.replay();
  }
  @After
  public void cleanup() {
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
  public void testPutGet() throws IOException, InterruptedException {
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
  public void testRoll() throws IOException, InterruptedException {
    log.shutdownWorker();
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
    // 67 files with TestLog.MAX_FILE_SIZE=1000
    Assert.assertEquals(78, logCount);
  }
  /**
   * After replay of the log, we should find the event because the put
   * was committed
   */
  @Test
  public void testPutCommit() throws IOException, InterruptedException {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long transactionID = ++this.transactionID;
    FlumeEventPointer eventPointerIn = log.put(transactionID, eventIn);
    log.commitPut(transactionID);
    log.close();
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    System.out.println(log.getNextFileID());
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
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Assert.assertNull(queue.removeHead());
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
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Assert.assertNull(queue.removeHead());
  }

  /**
   * After replay of the log, we should the event because the take
   * was rolled back
   */
  @Test
  public void testPutTakeRollback() throws IOException, InterruptedException {
    FlumeEvent eventIn = TestUtils.newPersistableEvent();
    long putTransactionID = ++transactionID;
    FlumeEventPointer eventPointerIn = log.put(putTransactionID, eventIn);
    log.commitPut(putTransactionID);
    long takeTransactionID = ++transactionID;
    log.take(takeTransactionID, eventPointerIn);
    log.rollback(takeTransactionID);
    log.close();
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    takeAndVerify(eventPointerIn, eventIn);
  }

  @Test
  public void testCommitNoPut() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.commitPut(putTransactionID);
    log.close();
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead();
    Assert.assertNull(eventPointerOut);
  }

  @Test
  public void testCommitNoTake() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.commitTake(putTransactionID);
    log.close();
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead();
    Assert.assertNull(eventPointerOut);
  }

  @Test
  public void testRollbackNoPutTake() throws IOException, InterruptedException {
    long putTransactionID = ++transactionID;
    log.rollback(putTransactionID);
    log.close();
    log = new Log(Long.MAX_VALUE, LogFile.MAX_FILE_SIZE, 1,
        checkpointDir, dataDirs);
    log.replay();
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead();
    Assert.assertNull(eventPointerOut);
  }

  private void takeAndVerify(FlumeEventPointer eventPointerIn,
      FlumeEvent eventIn) throws IOException, InterruptedException {
    FlumeEventQueue queue = log.getFlumeEventQueue();
    FlumeEventPointer eventPointerOut = queue.removeHead();
    Assert.assertNotNull(eventPointerOut);
    Assert.assertNull(queue.removeHead());
    Assert.assertEquals(eventPointerIn, eventPointerOut);
    Assert.assertEquals(eventPointerIn.hashCode(), eventPointerOut.hashCode());
    FlumeEvent eventOut = log.get(eventPointerOut);
    Assert.assertNotNull(eventOut);
    Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
    Assert.assertArrayEquals(eventIn.getBody(), eventOut.getBody());
  }
}
