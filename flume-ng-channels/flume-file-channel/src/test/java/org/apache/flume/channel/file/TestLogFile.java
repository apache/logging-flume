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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
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
    logFileWriter = LogFileFactory.getWriter(dataFile, fileID, 1000);
  }
  @After
  public void cleanup() throws IOException {
    try {
      logFileWriter.close();
    } finally {
      FileUtils.deleteQuietly(dataDir);
    }
  }
  @Test
  public void testPutGet() throws InterruptedException, IOException {
    final List<Throwable> errors =
        Collections.synchronizedList(new ArrayList<Throwable>());
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    final LogFile.RandomReader logFileReader =
        LogFileFactory.getRandomReader(dataFile);
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
      executorService.submit(new Runnable() {
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
      });
    }
    // first try and throw failures
    for(Throwable throwable : errors) {
      Throwables.propagateIfInstanceOf(throwable, AssertionError.class);
    }
    // then throw errors
    for(Throwable throwable : errors) {
      Throwables.propagate(throwable);
    }
    Assert.assertTrue(logFileWriter.isRollRequired(ByteBuffer.allocate(0)));
  }
  @Test
  public void testReader() throws InterruptedException, IOException {
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
        LogFileFactory.getSequentialReader(dataFile);
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
}
