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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.flume.channel.file.proto.ProtosFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestLogFile {
  private int fileID;
  private long transactionID;
  private LogFile.Writer logFileWriter;
  private File dataDir;
  private File dataFile;
  @Before
  public void setup() throws IOException {
    fileID = 1;
    transactionID = 1L;
    dataDir = Files.createTempDir();
    dataFile = new File(dataDir, String.valueOf(fileID));
    Assert.assertTrue(dataDir.isDirectory());
    logFileWriter = LogFileFactory.getWriter(dataFile, fileID,
        Integer.MAX_VALUE, null, null, null, Long.MAX_VALUE, true, 0);
  }
  @After
  public void cleanup() throws IOException {
    try {
      if(logFileWriter != null) {
        logFileWriter.close();
      }
    } finally {
      FileUtils.deleteQuietly(dataDir);
    }
  }
  @Test
  public void testWriterRefusesToOverwriteFile() throws IOException {
    Assert.assertTrue(dataFile.isFile() || dataFile.createNewFile());
    try {
      LogFileFactory.getWriter(dataFile, fileID, Integer.MAX_VALUE, null, null,
          null, Long.MAX_VALUE, true, 0);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals("File already exists " + dataFile.getAbsolutePath(),
          e.getMessage());
    }
  }
  @Test
  public void testWriterFailsWithDirectory() throws IOException {
    FileUtils.deleteQuietly(dataFile);
    Assert.assertFalse(dataFile.exists());
    Assert.assertTrue(dataFile.mkdirs());
    try {
      LogFileFactory.getWriter(dataFile, fileID, Integer.MAX_VALUE, null, null,
          null, Long.MAX_VALUE, true, 0);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals("File already exists " + dataFile.getAbsolutePath(),
          e.getMessage());
    }
  }
  @Test
  public void testPutGet() throws InterruptedException, IOException {
    final List<Throwable> errors =
        Collections.synchronizedList(new ArrayList<Throwable>());
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    CompletionService<Void> completionService = new ExecutorCompletionService
      <Void>(executorService);
    final LogFile.RandomReader logFileReader =
        LogFileFactory.getRandomReader(dataFile, null, true);
    for (int i = 0; i < 1000; i++) {
      // first try and throw failures
      synchronized (errors) {
        for(Throwable throwable : errors) {
          Throwables.propagateIfInstanceOf(throwable, AssertionError.class);
        }
        // then throw errors
        for(Throwable throwable : errors) {
          Throwables.propagate(throwable);
        }
      }
      final FlumeEvent eventIn = TestUtils.newPersistableEvent();
      final Put put = new Put(++transactionID, WriteOrderOracle.next(),
          eventIn);
      ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
      FlumeEventPointer ptr = logFileWriter.put(bytes);
      final int offset = ptr.getOffset();
      completionService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            FlumeEvent eventOut = logFileReader.get(offset);
            Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
            Assert.assertTrue(Arrays.equals(eventIn.getBody(), eventOut.getBody()));
          } catch(Throwable throwable) {
            synchronized (errors) {
              errors.add(throwable);
            }
          }
        }
      }, null);
    }

    for(int i = 0; i < 1000; i++) {
      completionService.take();
    }
    // first try and throw failures
    for(Throwable throwable : errors) {
      Throwables.propagateIfInstanceOf(throwable, AssertionError.class);
    }
    // then throw errors
    for(Throwable throwable : errors) {
      Throwables.propagate(throwable);
    }
  }
  @Test
  public void testReader() throws InterruptedException, IOException,
    CorruptEventException {
    Map<Integer, Put> puts = Maps.newHashMap();
    for (int i = 0; i < 1000; i++) {
      FlumeEvent eventIn = TestUtils.newPersistableEvent();
      Put put = new Put(++transactionID, WriteOrderOracle.next(),
          eventIn);
      ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
      FlumeEventPointer ptr = logFileWriter.put(bytes);
      puts.put(ptr.getOffset(), put);
    }
    LogFile.SequentialReader reader =
        LogFileFactory.getSequentialReader(dataFile, null, true);
    LogRecord entry;
    while((entry = reader.next()) != null) {
      Integer offset = entry.getOffset();
      TransactionEventRecord record = entry.getEvent();
      Put put = puts.get(offset);
      FlumeEvent eventIn = put.getEvent();
      Assert.assertEquals(put.getTransactionID(), record.getTransactionID());
      Assert.assertTrue(record instanceof Put);
      FlumeEvent eventOut = ((Put)record).getEvent();
      Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
      Assert.assertTrue(Arrays.equals(eventIn.getBody(), eventOut.getBody()));
    }
  }

  @Test
  public void testReaderOldMetaFile() throws InterruptedException,
    IOException, CorruptEventException {
    Map<Integer, Put> puts = Maps.newHashMap();
    for (int i = 0; i < 1000; i++) {
      FlumeEvent eventIn = TestUtils.newPersistableEvent();
      Put put = new Put(++transactionID, WriteOrderOracle.next(),
              eventIn);
      ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
      FlumeEventPointer ptr = logFileWriter.put(bytes);
      puts.put(ptr.getOffset(), put);
    }
    //rename the meta file to meta.old
    File metadataFile = Serialization.getMetaDataFile(dataFile);
    File oldMetadataFile = Serialization.getOldMetaDataFile(dataFile);
    if (!metadataFile.renameTo(oldMetadataFile)) {
      Assert.fail("Renaming to meta.old failed");
    }
    LogFile.SequentialReader reader =
            LogFileFactory.getSequentialReader(dataFile, null, true);
    Assert.assertTrue(metadataFile.exists());
    Assert.assertFalse(oldMetadataFile.exists());
    LogRecord entry;
    while ((entry = reader.next()) != null) {
      Integer offset = entry.getOffset();
      TransactionEventRecord record = entry.getEvent();
      Put put = puts.get(offset);
      FlumeEvent eventIn = put.getEvent();
      Assert.assertEquals(put.getTransactionID(), record.getTransactionID());
      Assert.assertTrue(record instanceof Put);
      FlumeEvent eventOut = ((Put) record).getEvent();
      Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
      Assert.assertTrue(Arrays.equals(eventIn.getBody(), eventOut.getBody()));
    }
  }

    @Test
  public void testReaderTempMetaFile() throws InterruptedException,
      IOException, CorruptEventException {
    Map<Integer, Put> puts = Maps.newHashMap();
    for (int i = 0; i < 1000; i++) {
      FlumeEvent eventIn = TestUtils.newPersistableEvent();
      Put put = new Put(++transactionID, WriteOrderOracle.next(),
              eventIn);
      ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
      FlumeEventPointer ptr = logFileWriter.put(bytes);
      puts.put(ptr.getOffset(), put);
    }
    //rename the meta file to meta.old
    File metadataFile = Serialization.getMetaDataFile(dataFile);
    File tempMetadataFile = Serialization.getMetaDataTempFile(dataFile);
    File oldMetadataFile = Serialization.getOldMetaDataFile(dataFile);
    oldMetadataFile.createNewFile(); //Make sure temp file is picked up.
    if (!metadataFile.renameTo(tempMetadataFile)) {
      Assert.fail("Renaming to meta.temp failed");
    }
    LogFile.SequentialReader reader =
            LogFileFactory.getSequentialReader(dataFile, null, true);
    Assert.assertTrue(metadataFile.exists());
    Assert.assertFalse(tempMetadataFile.exists());
    Assert.assertFalse(oldMetadataFile.exists());
    LogRecord entry;
    while ((entry = reader.next()) != null) {
      Integer offset = entry.getOffset();
      TransactionEventRecord record = entry.getEvent();
      Put put = puts.get(offset);
      FlumeEvent eventIn = put.getEvent();
      Assert.assertEquals(put.getTransactionID(), record.getTransactionID());
      Assert.assertTrue(record instanceof Put);
      FlumeEvent eventOut = ((Put) record).getEvent();
      Assert.assertEquals(eventIn.getHeaders(), eventOut.getHeaders());
      Assert.assertTrue(Arrays.equals(eventIn.getBody(), eventOut.getBody()));
    }
  }
  @Test
  public void testWriteDelimitedTo() throws IOException {
    if(dataFile.isFile()) {
      Assert.assertTrue(dataFile.delete());
    }
    Assert.assertTrue(dataFile.createNewFile());
    ProtosFactory.LogFileMetaData.Builder metaDataBuilder =
        ProtosFactory.LogFileMetaData.newBuilder();
    metaDataBuilder.setVersion(1);
    metaDataBuilder.setLogFileID(2);
    metaDataBuilder.setCheckpointPosition(3);
    metaDataBuilder.setCheckpointWriteOrderID(4);
    LogFileV3.writeDelimitedTo(metaDataBuilder.build(), dataFile);
    ProtosFactory.LogFileMetaData metaData = ProtosFactory.LogFileMetaData.
        parseDelimitedFrom(new FileInputStream(dataFile));
    Assert.assertEquals(1, metaData.getVersion());
    Assert.assertEquals(2, metaData.getLogFileID());
    Assert.assertEquals(3, metaData.getCheckpointPosition());
    Assert.assertEquals(4, metaData.getCheckpointWriteOrderID());
  }

  @Test (expected = CorruptEventException.class)
  public void testPutGetCorruptEvent() throws Exception {
    final LogFile.RandomReader logFileReader =
      LogFileFactory.getRandomReader(dataFile, null, true);
    final FlumeEvent eventIn = TestUtils.newPersistableEvent(2500);
    final Put put = new Put(++transactionID, WriteOrderOracle.next(),
      eventIn);
    ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
    FlumeEventPointer ptr = logFileWriter.put(bytes);
    logFileWriter.commit(TransactionEventRecord.toByteBuffer(new Commit
      (transactionID, WriteOrderOracle.next())));
    logFileWriter.sync();
    final int offset = ptr.getOffset();
    RandomAccessFile writer = new RandomAccessFile(dataFile, "rw");
    writer.seek(offset + 1500);
    writer.write((byte) 45);
    writer.write((byte) 12);
    writer.getFD().sync();
    logFileReader.get(offset);

    // Should have thrown an exception by now.
    Assert.fail();

  }

  @Test (expected = NoopRecordException.class)
  public void testPutGetNoopEvent() throws Exception {
    final LogFile.RandomReader logFileReader =
      LogFileFactory.getRandomReader(dataFile, null, true);
    final FlumeEvent eventIn = TestUtils.newPersistableEvent(2500);
    final Put put = new Put(++transactionID, WriteOrderOracle.next(),
      eventIn);
    ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
    FlumeEventPointer ptr = logFileWriter.put(bytes);
    logFileWriter.commit(TransactionEventRecord.toByteBuffer(new Commit
      (transactionID, WriteOrderOracle.next())));
    logFileWriter.sync();
    final int offset = ptr.getOffset();
    LogFile.OperationRecordUpdater updater = new LogFile
      .OperationRecordUpdater(dataFile);
    updater.markRecordAsNoop(offset);
    logFileReader.get(offset);

    // Should have thrown an exception by now.
    Assert.fail();
  }

  @Test
  public void testOperationRecordUpdater() throws Exception {
    File tempDir = Files.createTempDir();
    File temp = new File(tempDir, "temp");
    final RandomAccessFile tempFile = new RandomAccessFile(temp, "rw");
    for(int i = 0; i < 5000; i++) {
      tempFile.write(LogFile.OP_RECORD);
    }
    tempFile.seek(0);
    LogFile.OperationRecordUpdater recordUpdater = new LogFile
      .OperationRecordUpdater(temp);
    //Convert every 10th byte into a noop byte
    for(int i = 0; i < 5000; i+=10) {
      recordUpdater.markRecordAsNoop(i);
    }
    recordUpdater.close();

    tempFile.seek(0);
    // Verify every 10th byte is actually a NOOP
    for(int i = 0; i < 5000; i+=10) {
      tempFile.seek(i);
      Assert.assertEquals(LogFile.OP_NOOP, tempFile.readByte());
    }

  }

  @Test
  public void testOpRecordUpdaterWithFlumeEvents() throws Exception{
    final FlumeEvent eventIn = TestUtils.newPersistableEvent(2500);
    final Put put = new Put(++transactionID, WriteOrderOracle.next(),
      eventIn);
    ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
    FlumeEventPointer ptr = logFileWriter.put(bytes);
    logFileWriter.commit(TransactionEventRecord.toByteBuffer(new Commit
      (transactionID, WriteOrderOracle.next())));
    logFileWriter.sync();
    final int offset = ptr.getOffset();
    LogFile.OperationRecordUpdater updater = new LogFile
      .OperationRecordUpdater(dataFile);
    updater.markRecordAsNoop(offset);
    RandomAccessFile fileReader = new RandomAccessFile(dataFile, "rw");
    Assert.assertEquals(LogFile.OP_NOOP, fileReader.readByte());
  }

  @Test
  public void testGroupCommit() throws Exception {
    final FlumeEvent eventIn = TestUtils.newPersistableEvent(250);
    final CyclicBarrier barrier = new CyclicBarrier(20);
    ExecutorService executorService = Executors.newFixedThreadPool(20);
    ExecutorCompletionService<Void> completionService = new
      ExecutorCompletionService<Void>(executorService);
    final LogFile.Writer writer = logFileWriter;
    final AtomicLong txnId = new AtomicLong(++transactionID);
    for (int i = 0; i < 20; i++) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(txnId.incrementAndGet(),
              WriteOrderOracle.next(), eventIn);
            ByteBuffer bytes = TransactionEventRecord.toByteBuffer(put);
            writer.put(bytes);
            writer.commit(TransactionEventRecord.toByteBuffer(
              new Commit(txnId.get(), WriteOrderOracle.next())));
            barrier.await();
            writer.sync();
          } catch (Exception ex) {
            Throwables.propagate(ex);
          }
          return null;
        }
      });
    }

    for(int i = 0; i < 20; i++) {
      completionService.take().get();
    }

    //At least 250*20, but can be higher due to serialization overhead
    Assert.assertTrue(logFileWriter.position() >= 5000);
    Assert.assertEquals(1, writer.getSyncCount());
    Assert.assertTrue(logFileWriter.getLastCommitPosition() ==
      logFileWriter.getLastSyncPosition());

    executorService.shutdown();

  }
}
