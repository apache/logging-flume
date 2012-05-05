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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.flume.channel.recoverable.memory.wal.WAL;
import org.apache.flume.channel.recoverable.memory.wal.WALEntry;
import org.apache.flume.channel.recoverable.memory.wal.WALReplayResult;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestWAL {

  private static final Logger logger = LoggerFactory
      .getLogger(TestWAL.class);

  private File dataDir;
  private WAL<Text> wal;

  @Before
  public void setup() throws IOException {
    dataDir = Files.createTempDir();
    Assert.assertTrue(dataDir.isDirectory());
    wal = new WAL<Text>(dataDir, Text.class);
  }
  @After
  public void teardown() throws IOException {
    wal.close();
    FileUtils.deleteQuietly(dataDir);
  }

  /**
   * Create a whole bunch of files and ensure they are cleaned up
   */
  @Test
  public void testRoll() throws IOException, InterruptedException {
    wal.close();
    wal = new WAL<Text>(dataDir, Text.class, 0L, 0L, 0L, 1L);
    long seqid = 0;
    List<String> expected = strings(100);
    for(String s : expected) {
      wal.writeEntry(new WALEntry<Text>(new Text(s), ++seqid));
      Thread.sleep(1);
      wal.writeSequenceID(seqid);
      Thread.sleep(1);
    }
    wal.writeSequenceID(Long.MAX_VALUE);
    Thread.sleep(1000L);
    wal.close();
    File seq = new File(dataDir, "seq");
    File[] seqFiles = seq.listFiles();
    Assert.assertNotNull(seqFiles);
    Assert.assertTrue(seqFiles.length < 5);
    File data = new File(dataDir, "data");
    File[] dataFiles = data.listFiles();
    Assert.assertNotNull(dataFiles);
    Assert.assertTrue(dataFiles.length < 5);
  }

  @Test
  public void testBasicReplay() throws IOException {
    long seqid = 0;
    List<String> expected = strings(100);
    for(String s : expected) {
      wal.writeEntry(new WALEntry<Text>(new Text(s), ++seqid));
    }
    wal.close();
    wal = new WAL<Text>(dataDir, Text.class);
    WALReplayResult<Text> result = wal.replay();
    Assert.assertEquals(100, result.getSequenceID());
    List<String> actual = toStringList(result.getResults());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testReplayAtOffset() throws IOException {
    long seqid = 0;
    List<String> expected = strings(100);
    for(String s : expected) {
      wal.writeEntry(new WALEntry<Text>(new Text(s), ++seqid));
    }
    wal.writeSequenceID(50);
    expected.remove(50);
    wal.close();
    wal = new WAL<Text>(dataDir, Text.class);
    WALReplayResult<Text> result = wal.replay();
    Assert.assertEquals(100, result.getSequenceID());
    List<String> actual = toStringList(result.getResults());
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(99, actual.size());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testReplayNone() throws IOException {
    long seqid = 0;
    List<String> expected = strings(100);
    for(String s : expected) {
      wal.writeEntry(new WALEntry<Text>(new Text(s), ++seqid));
      wal.writeSequenceID(seqid);
    }
    wal.close();
    wal = new WAL<Text>(dataDir, Text.class);
    WALReplayResult<Text> result = wal.replay();
    Assert.assertEquals(expected.size(), result.getSequenceID());
    List<String> actual = toStringList(result.getResults());
    Assert.assertEquals(Collections.EMPTY_LIST, actual);
  }

  @Test
  public void testThreadedAppend() throws IOException, InterruptedException {
    int numThreads = 10;
    final CountDownLatch startLatch = new CountDownLatch(numThreads);
    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final AtomicLong seqid = new AtomicLong(0);
    final List<String> globalExpected = Collections.synchronizedList(new ArrayList<String>());
    final List<Exception> errors = Collections.synchronizedList(new ArrayList<Exception>());
    for (int i = 0; i < numThreads; i++) {
      final int id = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            List<String> expected = strings(100);
            globalExpected.addAll(expected);
            startLatch.countDown();
            startLatch.await();
            // half batch, half do not
            if(id % 2 == 0) {
              for(String s : expected) {
                wal.writeEntry(new WALEntry<Text>(new Text(s), seqid.incrementAndGet()));
              }
            } else {
              List<WALEntry<Text>> batch = Lists.newArrayList();
              for(String s : expected) {
                batch.add(new WALEntry<Text>(new Text(s), seqid.incrementAndGet()));
              }
              wal.writeEntries(batch);
            }
          } catch (Exception e) {
            logger.warn("Error doing appends", e);
            errors.add(e);
          } finally {
            stopLatch.countDown();
          }
        }
      };
      t.setDaemon(true);
      t.start();
    }
    Assert.assertTrue(stopLatch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(Collections.EMPTY_LIST, errors);
    wal.close();
    wal = new WAL<Text>(dataDir, Text.class);
    WALReplayResult<Text> result = wal.replay();
    Assert.assertEquals(1000, result.getSequenceID());
    List<String> actual = toStringList(result.getResults());
    // we don't know what order the items threads will be able to
    // append to the wal, so sort to the lists to make then sensible
    Collections.sort(actual);
    Collections.sort(globalExpected);
    Assert.assertEquals(globalExpected, actual);
  }

  @Test(expected=IOException.class)
  public void testInvalidReadClass() throws IOException {
    wal.writeEntry(new WALEntry<Text>(new Text(""), 1));
    wal.close();
    new WAL<IntWritable>(dataDir, IntWritable.class);
  }

  @Test(expected=NullPointerException.class)
  public void testCloseSingle() throws IOException {
    wal.close();
    wal.writeEntry(new WALEntry<Text>(new Text(""), 1));
  }

  @SuppressWarnings("unchecked")
  @Test(expected=NullPointerException.class)
  public void testCloseList() throws IOException {
    wal.close();
    wal.writeEntries(Lists.newArrayList(new WALEntry<Text>(new Text(""), 1)));
  }

  @Test(expected=NullPointerException.class)
  public void testCloseSequenceID() throws IOException {
    wal.close();
    wal.writeSequenceID(1L);
  }

  private static List<String> strings(int num) {
    List<String> result = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      String s = Integer.toString(num);
      result.add(s);
    }
    return result;
  }
  private static List<String> toStringList(List<WALEntry<Text>> list) {
    List<String> result = Lists.newArrayList();
    for(WALEntry<Text> entry : list) {
      result.add(entry.getData().toString());
    }
    return result;
  }

  public static void main(String[] args) throws IOException {
    Preconditions.checkPositionIndex(0, args.length,
        "size  of event is a required arg");
    Preconditions.checkPositionIndex(1, args.length,
        "batch size is a required arg");

    int size = Integer.parseInt(args[0]);
    int batchSize = Integer.parseInt(args[1]);

    byte[] buffer = new byte[size];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte)'A';
    }
    BytesWritable bytes = new BytesWritable(buffer);
    List<WALEntry<BytesWritable>> batch = Lists.newArrayList();
    long seqid = 0;
    long numBytes = 0;
    long count = 0;
    long start = System.currentTimeMillis();
    File dataDir = Files.createTempDir();
    try {
      WAL<BytesWritable>  wal = new WAL<BytesWritable>(dataDir, BytesWritable.class);
      while(true) {
        batch.clear();
        for (int i = 0; i < batchSize; i++) {
          batch.add(new  WALEntry<BytesWritable>(bytes, seqid++));
        }
        wal.writeEntries(batch);
        count += batchSize;
        numBytes += buffer.length * batchSize;

        long expired = System.currentTimeMillis() - start;
        if(expired > 10000L) {
          start = System.currentTimeMillis();
          System.out.println(String.format("Events/s %d, MB/s %4.2f", (count/10),
              (double)(numBytes/1024L/1024L)/(double)(expired/1000L)));
          numBytes = count = 0;
        }
      }
    } finally {
      FileUtils.deleteQuietly(dataDir);
    }
  }
}
