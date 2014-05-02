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
import static org.fest.reflect.core.Reflection.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flume.channel.file.FileChannel.FileBackedTransaction;
import org.apache.flume.channel.file.FlumeEventQueue.InflightEventWrapper;
import org.apache.flume.event.EventBuilder;

public class TestFileChannel extends TestFileChannelBase {

  private static final Logger LOG = LoggerFactory
          .getLogger(TestFileChannel.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }
  @After
  public void teardown() {
    super.teardown();
  }

  @Test
  public void testNegativeCapacities() {
    Map<String, String> parms = Maps.newHashMap();
    parms.put(FileChannelConfiguration.CAPACITY, "-3");
    parms.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "-1");
    parms.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "-2");
    FileChannel channel = createFileChannel(parms);

    Assert.assertTrue(field("capacity")
            .ofType(Integer.class)
            .in(channel).get() > 0);

    Assert.assertTrue(field("transactionCapacity")
            .ofType(Integer.class)
            .in(channel).get() > 0);

    Assert.assertTrue(field("checkpointInterval")
            .ofType(Long.class)
            .in(channel).get() > 0);
  }

  @Test
  public void testFailAfterTakeBeforeCommit() throws Throwable {
    final FileChannel channel = createFileChannel();
    channel.start();
    final Set<String> eventSet =
            putEvents(channel, "testTakeFailBeforeCommit", 5, 5);
    Transaction tx = channel.getTransaction();
    takeWithoutCommit(channel, tx, 2);
    //Simulate multiple sources, so separate thread - txns are thread local,
    //so a new txn wont be created here unless it is in a different thread.
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        Transaction tx = channel.getTransaction();
        takeWithoutCommit(channel, tx, 3);
      }
    }).get();
    forceCheckpoint(channel);
    channel.stop();
    //Simulate a sink, so separate thread.
    try {
      Executors.newSingleThreadExecutor().submit(new Runnable() {
        @Override
        public void run() {
          FileChannel channel = createFileChannel();
          channel.start();
          Set<String> output = null;
          try {
            output = takeEvents(channel, 5);
          } catch (Exception e) {
            Throwables.propagate(e);
          }
          compareInputAndOut(eventSet, output);
          channel.stop();
        }
      }).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testFailAfterPutCheckpointCommit() throws Throwable {
    final Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "10000");
    final FileChannel channel = createFileChannel(overrides);
    channel.start();
    Transaction tx = channel.getTransaction();
    final Set<String> input = putWithoutCommit(channel, tx, "failAfterPut", 3);
    //Simulate multiple sources, so separate thread - txns are thread local,
    //so a new txn wont be created here unless it is in a different thread.
    final CountDownLatch latch = new CountDownLatch(1);
    Executors.newSingleThreadExecutor().submit(
            new Runnable() {
              @Override
              public void run() {
                Transaction tx = channel.getTransaction();
                input.addAll(putWithoutCommit(channel, tx, "failAfterPut", 3));
                try {
                  latch.await();
                  tx.commit();
                } catch (InterruptedException e) {
                  tx.rollback();
                  Throwables.propagate(e);
                } finally {
                  tx.close();
                }
              }
            });
    forceCheckpoint(channel);
    tx.commit();
    tx.close();
    latch.countDown();
    Thread.sleep(2000);
    channel.stop();

    final Set<String> out = Sets.newHashSet();
    //Simulate a sink, so separate thread.
    try {
      Executors.newSingleThreadExecutor().submit(new Runnable() {
        @Override
        public void run() {
          try {
            FileChannel channel = createFileChannel();
            channel.start();
            out.addAll(takeEvents(channel, 6));
            channel.stop();
          } catch (Exception ex) {
            Throwables.propagate(ex);
          }
        }
      }).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }

  }

  @Test
  public void testReconfigure() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = Sets.newHashSet();
    try {
      while(true) {
        in.addAll(putEvents(channel, "reconfig", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("The channel has reached it's capacity. "
          + "This might be the result of a sink on the channel having too "
          + "low of batch size, a downstream system running slower than "
          + "normal, or that the channel capacity is just too low. [channel="
          + channel.getName()+"]", e.getMessage());
    }
    Configurables.configure(channel, createContext());
    Set<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    compareInputAndOut(in, out);
  }
  @Test
  public void testPut() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    // should find no items
    int found = takeEvents(channel, 1, 5).size();
    Assert.assertEquals(0, found);
    Set<String> expected = Sets.newHashSet();
    expected.addAll(putEvents(channel, "unbatched", 1, 5));
    expected.addAll(putEvents(channel, "batched", 5, 5));
    Set<String> actual = takeEvents(channel, 1);
    compareInputAndOut(expected, actual);
  }
  @Test
  public void testCommitAfterNoPutTake() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    transaction.commit();
    transaction.close();

    // ensure we can reopen log with no error
    channel.stop();
    channel = createFileChannel();
    channel.start();
    Assert.assertTrue(channel.isOpen());
    transaction = channel.getTransaction();
    transaction.begin();
    Assert.assertNull(channel.take());
    transaction.commit();
    transaction.close();
  }
  @Test
  public void testCapacity() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(5));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(5));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    fillChannel(channel, "fillup");
    // take an event, roll it back, and
    // then make sure a put fails
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    Event event = channel.take();
    Assert.assertNotNull(event);
    transaction.rollback();
    transaction.close();
    // ensure the take the didn't change the state of the capacity
    Assert.assertEquals(0, fillChannel(channel, "capacity").size());
    // ensure we the events back
    Assert.assertEquals(5, takeEvents(channel, 1, 5).size());
  }
  /**
   * This test is here to make sure we can replay a full queue
   * when we have a PUT with a lower txid than the take which
   * made that PUT possible. Here we fill up the queue so
   * puts will block. Start the put (which assigns a txid)
   * and while it's blocking initiate a take. That will
   * allow the PUT to take place but at a lower txid
   * than the take and additionally with pre-FLUME-1432 with
   * the same timestamp. After FLUME-1432 the PUT will have a
   * lower txid but a higher write order id and we can see
   * which event occurred first.
   */
  @Test
  public void testRaceFoundInFLUME1432() throws Exception {
    // the idea here is we will fill up the channel
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(10L));
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(10));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    fillChannel(channel, "fillup");
    // then do a put which will block but it will be assigned a tx id
    Future<String> put = Executors.newSingleThreadExecutor()
            .submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Set<String> result = putEvents(channel, "blocked-put", 1, 1);
        Assert.assertTrue(result.toString(), result.size() == 1);
        Iterator<String> iter = result.iterator();
        return iter.next();
      }
    });
    Thread.sleep(1000L); // ensure the put has started and is blocked
    // after which we do a take, will have a tx id after the put
    Set<String> result = takeEvents(channel, 1, 1);
    Assert.assertTrue(result.toString(), result.size() == 1);
    String putmsg = put.get();
    Assert.assertNotNull(putmsg);
    String takemsg = result.iterator().next();
    Assert.assertNotNull(takemsg);
    LOG.info("Got: put " + putmsg + ", take " + takemsg);
    channel.stop();
    channel = createFileChannel(overrides);
    // now when we replay, the transaction the put will be ordered
    // before the take when we used the txid as an order of operations
    channel.start();
    Assert.assertTrue(channel.isOpen());
  }
  @Test
  public void testThreaded() throws IOException, InterruptedException {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    int numThreads = 10;
    final CountDownLatch producerStopLatch = new CountDownLatch(numThreads);
    final CountDownLatch consumerStopLatch = new CountDownLatch(numThreads);
    final List<Exception> errors = Collections
            .synchronizedList(new ArrayList<Exception>());
    final Set<String> expected = Collections.synchronizedSet(
            new HashSet<String>());
    final Set<String> actual = Collections.synchronizedSet(
            new HashSet<String>());
    for (int i = 0; i < numThreads; i++) {
      final int id = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            if (id % 2 == 0) {
              expected.addAll(putEvents(channel, Integer.toString(id), 1, 5));
            } else {
              expected.addAll(putEvents(channel, Integer.toString(id), 5, 5));
            }
            LOG.info("Completed some puts " + expected.size());
          } catch (Exception e) {
            LOG.error("Error doing puts", e);
            errors.add(e);
          } finally {
            producerStopLatch.countDown();
          }
        }
      };
      t.setDaemon(true);
      t.start();
    }
    for (int i = 0; i < numThreads; i++) {
      final int id = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            while(!producerStopLatch.await(1, TimeUnit.SECONDS) ||
                expected.size() > actual.size()) {
              if (id % 2 == 0) {
                actual.addAll(takeEvents(channel, 1, Integer.MAX_VALUE));
              } else {
                actual.addAll(takeEvents(channel, 5, Integer.MAX_VALUE));
              }
            }
            if(actual.isEmpty()) {
              LOG.error("Found nothing!");
            } else {
              LOG.info("Completed some takes " + actual.size());
            }
          } catch (Exception e) {
            LOG.error("Error doing takes", e);
            errors.add(e);
          } finally {
            consumerStopLatch.countDown();
          }
        }
      };
      t.setDaemon(true);
      t.start();
    }
    Assert.assertTrue("Timed out waiting for producers",
            producerStopLatch.await(30, TimeUnit.SECONDS));
    Assert.assertTrue("Timed out waiting for consumer",
            consumerStopLatch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(Collections.EMPTY_LIST, errors);
    compareInputAndOut(expected, actual);
  }
  @Test
  public void testLocking() throws IOException {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    FileChannel fileChannel = createFileChannel();
    fileChannel.start();
    Assert.assertTrue(!fileChannel.isOpen());
  }


  /**
   * Test contributed by Brock Noland during code review.
   * @throws Exception
   */
  @Test
  public void testTakeTransactionCrossingCheckpoint() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "10000");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    Set<String> out = Sets.newHashSet();
    // now take one item off the channel
    Transaction tx = channel.getTransaction();
    out.addAll(takeWithoutCommit(channel, tx, 1));
    // sleep so a checkpoint occurs. take is before
    // and commit is after the checkpoint
    forceCheckpoint(channel);
    tx.commit();
    tx.close();
    channel.stop();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    // we should not geet the item we took of the queue above
    Set<String> out2 = takeEvents(channel, 1, Integer.MAX_VALUE);
    channel.stop();
    in.removeAll(out);
    compareInputAndOut(in, out2);
  }

  @Test
  public void testPutForceCheckpointCommitReplay() throws Exception{
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(2));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(2));
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "10000");
    FileChannel channel = createFileChannel(overrides);
    channel.start();
    //Force a checkpoint by committing a transaction
    Transaction tx = channel.getTransaction();
    Set<String> in = putWithoutCommit(channel, tx, "putWithoutCommit", 1);
    forceCheckpoint(channel);
    tx.commit();
    tx.close();
    channel.stop();

    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = takeEvents(channel, 1);
    compareInputAndOut(in, out);
    channel.stop();

  }

  @Test
  public void testPutCheckpointCommitCheckpointReplay() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(2));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(2));
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "10000");
    FileChannel channel = createFileChannel(overrides);
    channel.start();
    //Force a checkpoint by committing a transaction
    Transaction tx = channel.getTransaction();
    Set<String> in = putWithoutCommit(channel, tx, "doubleCheckpoint", 1);
    forceCheckpoint(channel);
    tx.commit();
    tx.close();
    forceCheckpoint(channel);
    channel.stop();

    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = takeEvents(channel, 5);
    compareInputAndOut(in, out);
    channel.stop();
  }

  @Test
  public void testReferenceCounts() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "10000");
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE, "150");
    final FileChannel channel = createFileChannel(overrides);
    channel.start();
    putEvents(channel, "testing-reference-counting", 1, 15);
    Transaction tx = channel.getTransaction();
    takeWithoutCommit(channel, tx, 10);
    forceCheckpoint(channel);
    tx.rollback();
    //Since we did not commit the original transaction. now we should get 15
    //events back.
    final Set<String> takenEvents = Sets.newHashSet();
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        try {
          takenEvents.addAll(takeEvents(channel, 15));
        } catch (Exception ex) {
          Throwables.propagate(ex);
        }
      }
    }).get();
    Assert.assertEquals(15, takenEvents.size());
  }

  // This test will fail without FLUME-1606.
  @Test
  public void testRollbackIncompleteTransaction() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL,
            String.valueOf(Integer.MAX_VALUE));
    final FileChannel channel = createFileChannel(overrides);
    channel.start();
    FileBackedTransaction tx = (FileBackedTransaction) channel.getTransaction();

    InflightEventWrapper inflightPuts =
            field("inflightPuts").ofType(InflightEventWrapper.class).in(
            field("queue").ofType(FlumeEventQueue.class).in(tx).get()).get();

    tx.begin();

    for (int i = 0; i < 100; i++) {
      channel.put(EventBuilder.withBody("TestEvent".getBytes()));
    }

    Assert.assertFalse(inflightPuts.getFileIDs().isEmpty());
    Assert.assertFalse(inflightPuts.getInFlightPointers().isEmpty());

    tx.rollback();
    tx.close();

    Assert.assertTrue(inflightPuts.getFileIDs().isEmpty());
    Assert.assertTrue(inflightPuts.getInFlightPointers().isEmpty());
    Assert.assertTrue(channel.getDepth() == 0);

    Set<String> in = putEvents(channel, "testing-rollbacks", 100, 100);

    tx = (FileBackedTransaction) channel.getTransaction();

    InflightEventWrapper inflightTakes =
            field("inflightTakes").ofType(InflightEventWrapper.class).in(
            field("queue").ofType(FlumeEventQueue.class).in(tx).get()).get();

    tx.begin();

    for (int i = 0; i < 100; i++) {
      channel.take();
    }

    Assert.assertFalse(inflightTakes.getFileIDs().isEmpty());
    Assert.assertFalse(inflightTakes.getInFlightPointers().isEmpty());

    tx.rollback();
    tx.close();


    Assert.assertTrue(inflightTakes.getFileIDs().isEmpty());
    Assert.assertTrue(inflightTakes.getInFlightPointers().isEmpty());
    Assert.assertTrue(channel.getDepth() == in.size());

  }

  @Test (expected = IllegalStateException.class)
  public void testChannelDiesOnCorruptEventFsync() throws Exception {
    testChannelDiesOnCorruptEvent(true);
  }


  @Test
  public void testChannelDiesOnCorruptEventNoFsync() throws
    Exception {
    testChannelDiesOnCorruptEvent(false);
  }



  private void testChannelDiesOnCorruptEvent(boolean fsyncPerTxn)
    throws Exception {
    Map<String, String> overrides = new HashMap<String, String>();
    overrides.put(FileChannelConfiguration.FSYNC_PER_TXN,
      String.valueOf(fsyncPerTxn));
    final FileChannel channel = createFileChannel(overrides);
    channel.start();
    putEvents(channel,"test-corrupt-event",100,100);
    for(File dataDir : dataDirs) {
      File[] files = dataDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if(!name.endsWith("meta") && !name.contains("lock")){
            return true;
          }
          return false;
        }
      });
      if (files != null && files.length > 0) {
        for (int j = 0; j < files.length; j++) {
          RandomAccessFile fileToCorrupt = new RandomAccessFile(files[0], "rw");
          fileToCorrupt.seek(50);
          fileToCorrupt.writeByte(234);
          fileToCorrupt.close();
        }
      }
    }
    Set<String> events;
    try {
      events = consumeChannel(channel, true);
    } catch (IllegalStateException ex) {
      // The rollback call in takeEvents() in TestUtils will cause an
      // IllegalArgumentException - and this should be tested to verify the
      // channel is completely stopped.
      Assert.assertTrue(ex.getMessage().contains("Log is closed"));
      throw ex;
    }
    if(fsyncPerTxn) {
      Assert.fail();
    } else {
      // The corrupt event must be missing, the rest should be
      // returned
      Assert.assertEquals(99, events.size());
    }
  }

}
