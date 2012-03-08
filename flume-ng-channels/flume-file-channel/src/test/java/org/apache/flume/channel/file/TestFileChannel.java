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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel.FileBackedTransaction;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileChannel {

  private static final Logger logger = LoggerFactory
      .getLogger(TestFileChannel.class);

  private FileChannel channel;

  @Before
  public void setUp() {
    channel = new FileChannel();
  }

  @Test(expected = IllegalStateException.class)
  public void testNoDirectory() {
    Event event = EventBuilder.withBody("Test event".getBytes());

    channel.put(event);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonExistantParent() {
    Event event = EventBuilder.withBody("Test event".getBytes());

    channel.setDirectory(new File("/i/do/not/exist"));
    channel.put(event);
  }

  @Test
  public void testGetTransaction() throws IOException {
    File tmpDir = new File("/tmp/flume-fc-test-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(tmpDir);

    channel.setDirectory(tmpDir);

    Transaction tx1 = channel.getTransaction();
    Assert.assertNotNull(tx1);

    Transaction tx2 = channel.getTransaction();
    Assert.assertNotNull(tx2);

    Assert.assertEquals(tx1, tx2);

    tx2.begin();
    Assert.assertEquals(tx2, channel.getTransaction());

    tx2.rollback();
    Assert.assertEquals(tx2, channel.getTransaction());

    tx2.close();
    Assert.assertFalse(tx2.equals(channel.getTransaction()));
  }

  /**
   * <p>
   * Ensure two threads calling {@link FileChannel#getTransaction()} get
   * different transactions back.
   * </p>
   * 
   */
  @Test
  public void testConcurrentGetTransaction() throws IOException,
      InterruptedException, ExecutionException {
    File tmpDir = new File("/tmp/flume-fc-test-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(tmpDir);
    final CyclicBarrier latch = new CyclicBarrier(2);

    channel.setDirectory(tmpDir);

    Callable<FileBackedTransaction> getTransRunnable = new Callable<FileBackedTransaction>() {

      @Override
      public FileBackedTransaction call() {
        Transaction tx = null;

        try {
          /*
           * Wait for all threads to pile up to prevent thread reuse in the
           * pool.
           */
          latch.await();
          tx = channel.getTransaction();
          /*
           * This await isn't strictly necessary but it guarantees both threads
           * entered and exited getTransaction() in lock step which simplifies
           * debugging.
           */
          latch.await();
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for threads", e);
          Assert.fail();
        } catch (BrokenBarrierException e) {
          logger.error("Barrier broken", e);
          Assert.fail();
        }

        return (FileBackedTransaction) tx;
      }

    };

    ExecutorService pool = Executors.newFixedThreadPool(2);

    Future<FileBackedTransaction> f1 = pool.submit(getTransRunnable);
    Future<FileBackedTransaction> f2 = pool.submit(getTransRunnable);

    FileBackedTransaction t1 = f1.get();
    FileBackedTransaction t2 = f2.get();

    Assert.assertNotSame("Transactions from different threads are different",
        t1, t2);
  }

  @Test
  public void testPut() throws IOException {
    File tmpDir = new File("/tmp/flume-fc-test-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(tmpDir);

    if (!tmpDir.mkdirs()) {
      throw new IOException("Unable to create test directory:" + tmpDir);
    }

    channel.setDirectory(tmpDir);

    /* Issue five one record transactions. */
    for (int i = 0; i < 5; i++) {
      Transaction transaction = channel.getTransaction();

      Assert.assertNotNull(transaction);

      try {
        transaction.begin();

        Event event = EventBuilder.withBody(("Test event" + i).getBytes());
        channel.put(event);

        transaction.commit();
      } catch (Exception e) {
        logger.error(
            "Failed to put event into file channel. Exception follows.", e);
        transaction.rollback();
        Assert.fail();
      } finally {
        transaction.close();
      }
    }

    /* Issue one five record transaction. */
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      for (int i = 0; i < 5; i++) {
        Event event = EventBuilder.withBody(("Test event" + i).getBytes());
        channel.put(event);
      }

      transaction.commit();
    } catch (Exception e) {
      logger.error("Failed to put event into file channel. Exception follows.",
          e);
      transaction.rollback();
      Assert.fail();
    } finally {
      transaction.close();
    }
  }

}
