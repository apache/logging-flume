/*
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

package org.apache.flume.channel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.UUID;

import org.apache.flume.*;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.channel.file.FileChannelConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public class TestSpillableMemoryChannel {

  private SpillableMemoryChannel channel;

  @Rule
  public TemporaryFolder fileChannelDir = new TemporaryFolder();

  private void configureChannel(Map<String, String> overrides) {
    Context context = new Context();
    File checkPointDir = fileChannelDir.newFolder("checkpoint");
    File dataDir = fileChannelDir.newFolder("data");
    context.put(FileChannelConfiguration.CHECKPOINT_DIR
            , checkPointDir.getAbsolutePath());
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir.getAbsolutePath());
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");

    if (overrides != null)
      context.putAll(overrides);

    Configurables.configure(channel, context);
  }

  private void reconfigureChannel(Map<String, String> overrides) {
    configureChannel(overrides);
    channel.stop();
    channel.start();
  }

  private void startChannel(Map<String, String> params) {
    configureChannel(params);
    channel.start();
  }

  // performs a hard restart of the channel... creates a new channel object
  private void restartChannel(Map<String, String> params) {
    channel.stop();
    setUp();
    startChannel(params);
  }


  static class NullFound extends RuntimeException {
    public int expectedValue;
    public NullFound(int expected) {
      super("Expected " + expected + ",  but null found");
      expectedValue = expected;
    }
  }

  static class TooManyNulls extends RuntimeException {
    private int nullsFound;
    public TooManyNulls(int count) {
      super("Total nulls found in thread ("
              + Thread.currentThread().getName() + ") : " + count);
      nullsFound = count;
    }
  }

  @Before
  public void setUp() {
    channel = new SpillableMemoryChannel();
    channel.setName("spillChannel-" + UUID.randomUUID() );
  }

  @After
  public void tearDown() {
    channel.stop();
  }

  private static void putN(int first, int count, AbstractChannel channel) {
    for (int i = 0; i < count; ++i) {
      channel.put(EventBuilder.withBody(String.valueOf(first++).getBytes()));
    }
  }

  private static void takeNull(AbstractChannel channel) {
      channel.take();
  }

  private static void takeN(int first, int count, AbstractChannel channel) {
    int last = first + count;
    for (int i = first; i < last; ++i) {
      Event e = channel.take();
      if (e == null) {
        throw new NullFound(i);
      }
      Event expected = EventBuilder.withBody( String.valueOf(i).getBytes() );
      Assert.assertArrayEquals(e.getBody(), expected.getBody());
    }
  }

  // returns the number of non null events found
  private static int takeN_NoCheck(int batchSize, AbstractChannel channel) {
    int i = 0;
    for (; i < batchSize; ++i) {
      Event e = channel.take();
      if (e == null) {
        try {
          Thread.sleep(0);
        } catch (InterruptedException ex)
        { /* ignore */ }
        return i;
      }
    }
    return i;
  }

  private static void transactionalPutN(int first, int count,
                                        AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      putN(first, count, channel);
      tx.commit();
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } finally {
      tx.close();
    }
  }

  private static void transactionalTakeN(int first, int count,
                                         AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      takeN(first, count, channel);
      tx.commit();
    } catch (NullFound e) {
      tx.commit();
      throw e;
    } catch (AssertionError e) {
      tx.rollback();
      throw e;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } finally {
      tx.close();
    }
  }

  private static int transactionalTakeN_NoCheck(int count
          , AbstractChannel channel)  {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      int eventCount = takeN_NoCheck(count, channel);
      tx.commit();
      return  eventCount;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } finally {
      tx.close();
    }
  }

  private static void transactionalTakeNull(int count, AbstractChannel channel) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    try {
      for (int i = 0; i < count; ++i)
        takeNull(channel);
      tx.commit();
    } catch (AssertionError e) {
      tx.rollback();
      throw e;
    } catch (RuntimeException e) {
      tx.rollback();
      throw e;
    } finally {
      tx.close();
    }
  }

  private Thread makePutThread(String threadName
          , final int first, final int count, final int batchSize
          , final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          int maxdepth = 0;
          StopWatch watch = new StopWatch();
          for (int i = first; i<first+count; i=i+batchSize) {
            transactionalPutN(i, batchSize, channel);
          }
          watch.elapsed();
        }
      };
  }

  private static Thread makeTakeThread(String threadName, final int first
        , final int count, final int batchSize, final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          StopWatch watch = new StopWatch();
          for (int i = first; i < first+count; ) {
            try {
              transactionalTakeN(i, batchSize, channel);
              i = i + batchSize;
            } catch (NullFound e) {
              i = e.expectedValue;
            }
          }
          watch.elapsed();
        }
      };
  }

  private static Thread makeTakeThread_noCheck(String threadName
        , final int totalEvents, final int batchSize, final AbstractChannel channel) {
    return
      new Thread(threadName) {
        public void run() {
          int batchSz = batchSize;
          StopWatch watch = new StopWatch();
          int i = 0, attempts = 0 ;
          while(i < totalEvents) {
              int remaining = totalEvents - i;
              batchSz = (remaining > batchSz) ? batchSz : remaining;
              int takenCount = transactionalTakeN_NoCheck(batchSz, channel);
              if(takenCount < batchSz) {
                try {
                  Thread.sleep(20);
                } catch (InterruptedException ex)
                { /* ignore */ }
              }
              i += takenCount;
              ++attempts;
              if(attempts  >  totalEvents * 3 ) {
                throw new TooManyNulls(attempts);
              }
          }
          watch.elapsed(" items = " + i + ", attempts = " + attempts);
        }
      };
  }

  @Test
  public void testPutTake() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "5");
    params.put("overflowCapacity", "5");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "5");
    startChannel(params);

    Transaction tx = channel.getTransaction();
    tx.begin();
    putN(0,2,channel);
    tx.commit();
    tx.close();

    tx = channel.getTransaction();
    tx.begin();
    takeN(0,2,channel);
    tx.commit();
    tx.close();
  }


  @Test
  public void testCapacityDisableOverflow()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "0");   // overflow is disabled effectively
    params.put("overflowTimeout", "0" );
    startChannel(params);

    transactionalPutN(0,2,channel);

    boolean threw = false;
    try {
      transactionalPutN(2,1,channel);
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expecting ChannelFullException to be thrown", threw);

    transactionalTakeN(0,2, channel);

    Transaction tx = channel.getTransaction();
    tx.begin();
    Assert.assertNull(channel.take());
    tx.commit();
    tx.close();
  }

  @Test
  public void testCapacityWithOverflow()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "4");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "3");
    params.put("overflowTimeout", "0");
    startChannel(params);

    transactionalPutN(1, 2, channel);
    transactionalPutN(3, 2, channel);
    transactionalPutN(5, 2, channel);

    boolean threw = false;
    try {
      transactionalPutN(7,2,channel);   // cannot fit in channel
    } catch (ChannelFullException e) {
      threw = true;
    }
    Assert.assertTrue("Expecting ChannelFullException to be thrown", threw);

    transactionalTakeN(1,2, channel);
    transactionalTakeN(3,2, channel);
    transactionalTakeN(5,2, channel);
  }

  @Test
  public void testRestart()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "2");
    params.put("overflowCapacity", "10");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "4");
    params.put("overflowTimeout", "0");
    startChannel(params);

    transactionalPutN(1, 2, channel);
    transactionalPutN(3, 2, channel);  // goes in overflow

    restartChannel(params);

    // from overflow, as in memory stuff should be lost
    transactionalTakeN(3,2, channel);

  }

  @Test
  public void testBasicStart() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10000000");
    params.put("overflowCapacity", "20000000");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );

    startChannel(params);

    transactionalPutN( 1,5,channel);
    transactionalPutN( 6,5,channel);
    transactionalPutN(11,5,channel); // these should go to overflow

    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
  }

  @Test
  public void testOverflow() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "20");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );

    startChannel(params);

    transactionalPutN( 1,5,channel);
    transactionalPutN( 6,5,channel);
    transactionalPutN(11,5,channel); // these should go to overflow

    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
  }

  @Test
  public void testDrainOrder() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "10");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "5");
    params.put("overflowTimeout", "1" );

    startChannel(params);

    transactionalPutN( 1,5,channel);
    transactionalPutN( 6,5,channel);
    transactionalPutN(11,5,channel); // into overflow
    transactionalPutN(16,5,channel); // into overflow

    transactionalTakeN(1, 1, channel);
    transactionalTakeN(2, 5,channel);
    transactionalTakeN(7, 4,channel);

    transactionalPutN( 20,2,channel);
    transactionalPutN( 22,3,channel);

    transactionalTakeN( 11,3,channel); // from overflow
    transactionalTakeN( 14,5,channel); // from overflow
    transactionalTakeN( 19,2,channel); // from overflow
  }

  @Test
  public void testByteCapacity()  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "1000");
    // configure to hold 8 events of 10 bytes each (plus 20% event header space)
    params.put("byteCapacity", "100");
    params.put("avgEventSize", "10");
    params.put("overflowCapacity", "20");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );
    startChannel(params);

    transactionalPutN(1, 8, channel);   // this wil max the byteCapacity
    transactionalPutN(9, 10, channel);
    transactionalPutN(19,10, channel);  // this will fill up the overflow

    boolean threw = false;
    try {
      transactionalPutN(11, 1, channel);  // into overflow
    } catch (ChannelFullException e) {
      threw = true;
    }
    Assert.assertTrue("byteCapacity did not throw as expected", threw);

  }

  @Test
  public void testDrainingOnChannelBoundary() {

    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "5");
    params.put("overflowCapacity", "15");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "10");
    params.put("overflowTimeout", "1" );
    startChannel(params);

    transactionalPutN(1, 5, channel);
    transactionalPutN(6, 5, channel);  // into overflow
    transactionalPutN(11, 5, channel);  // into overflow
    transactionalPutN(16, 5, channel);  // into overflow

    transactionalTakeN(1, 3, channel);

    Transaction tx = channel.getTransaction();
    tx.begin();
    takeN(4, 2, channel);
    takeNull(channel);  // expect null since next event is in overflow
    tx.commit();
    tx.close();

    transactionalTakeN(6, 5, channel);  // from overflow

    transactionalTakeN(11, 5, channel); // from overflow
    transactionalTakeN(16, 2,channel); // from overflow

    transactionalPutN(21, 5, channel);

    tx = channel.getTransaction();
    tx.begin();
    takeN(18,3, channel);              // from overflow
    takeNull(channel);  // expect null since next event is in primary
    tx.commit();
    tx.close();

    transactionalTakeN(21, 5, channel);
  }

  @Test
  public void testRollBack() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "100");
    params.put("overflowCapacity", "900");
    params.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "900");
    params.put("overflowTimeout", "0");
    startChannel(params);


    //1 Rollback for Puts
    transactionalPutN(1,5, channel);
    Transaction tx = channel.getTransaction();
    tx.begin();
    putN(6, 5, channel);
    tx.rollback();
    tx.close();

    transactionalTakeN(1, 5, channel);
    transactionalTakeNull(2, channel);

    //2.  verify things back to normal after put rollback
    transactionalPutN(11, 5, channel);
    transactionalTakeN(11,5,channel);


    //3 Rollback for Takes
    transactionalPutN(16, 5, channel);
    tx = channel.getTransaction();
    tx.begin();
    takeN(16, 5, channel);
    takeNull(channel);
    tx.rollback();
    tx.close();

    transactionalTakeN_NoCheck(5, channel);

    //4.  verify things back to normal after take rollback
    transactionalPutN(21,5, channel);
    transactionalTakeN(21,5,channel);
  }


  @Test
  public void testReconfigure()  {
    //1) bring up with small capacity
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "0");
    params.put("overflowTimeout", "0");
    startChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly"
            , channel.getOverflowTimeout() == 0);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly"
            , channel.getMemoryCapacity() == 10);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly"
            , channel.isOverflowDisabled() );

    transactionalPutN(1, 10, channel);
    boolean threw = false;
    try {
      transactionalPutN(11, 10, channel);  // should throw an error
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the channel to fill up and throw an exception, "
            + "but it did not throw", threw);


    //2) Resize and verify
    params = new HashMap<String, String>();
    params.put("memoryCapacity", "20");
    params.put("overflowCapacity", "0");
    reconfigureChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly"
        , channel.getOverflowTimeout() == SpillableMemoryChannel.defaultOverflowTimeout);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly"
        , channel.getMemoryCapacity() == 20);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly"
        , channel.isOverflowDisabled() );

    // pull out the values inserted prior to reconfiguration
    transactionalTakeN(1, 10, channel);

    transactionalPutN(11, 10, channel);
    transactionalPutN(21, 10, channel);

    threw = false;
    try {
      transactionalPutN(31, 10, channel);  // should throw an error
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the channel to fill up and throw an exception, "
            + "but it did not throw", threw);

    transactionalTakeN(11, 10, channel);
    transactionalTakeN(21, 10, channel);


    // 3) Reconfigure with empty config and verify settings revert to default
    params = new HashMap<String, String>();
    reconfigureChannel(params);

    Assert.assertTrue("overflowTimeout setting did not reconfigure correctly"
      , channel.getOverflowTimeout() == SpillableMemoryChannel.defaultOverflowTimeout);
    Assert.assertTrue("memoryCapacity did not reconfigure correctly"
      , channel.getMemoryCapacity() == SpillableMemoryChannel.defaultMemoryCapacity);
    Assert.assertTrue("overflowCapacity did not reconfigure correctly"
      , channel.getOverflowCapacity() == SpillableMemoryChannel.defaultOverflowCapacity);
    Assert.assertFalse("overflowCapacity did not reconfigure correctly"
      , channel.isOverflowDisabled());


    // 4) Reconfiguring of  overflow
    params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "10");
    params.put("transactionCapacity", "5");
    params.put("overflowTimeout", "1");
    reconfigureChannel(params);

    transactionalPutN( 1,5, channel);
    transactionalPutN( 6,5, channel);
    transactionalPutN(11,5, channel);
    transactionalPutN(16,5, channel);
    threw=false;
    try {
      // should error out as both primary & overflow are full
      transactionalPutN(21,5, channel);
    } catch (ChannelException e) {
      threw = true;
    }
    Assert.assertTrue("Expected the last insertion to fail, but it didn't."
            , threw);

    // reconfig the overflow
    params = new HashMap<String, String>();
    params.put("memoryCapacity", "10");
    params.put("overflowCapacity", "20");
    params.put("transactionCapacity", "10");
    params.put("overflowTimeout", "1");
    reconfigureChannel(params);

    // should succeed now as we have made room in the overflow
    transactionalPutN(21,5, channel);

    transactionalTakeN(1,10, channel);
    transactionalTakeN(11,5, channel);
    transactionalTakeN(16, 5, channel);
    transactionalTakeN(21, 5, channel);
  }

  @Test
  public void testParallelSingleSourceAndSink() throws InterruptedException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "1000020");
    params.put("overflowCapacity",   "0");
    params.put("overflowTimeout", "3");
    startChannel(params);

    // run source and sink concurrently
    Thread sourceThd = makePutThread("src", 1, 500000, 100, channel);
    Thread sinkThd = makeTakeThread("sink",  1, 500000, 100, channel);

    StopWatch watch = new StopWatch();

    sinkThd.start();
    sourceThd.start();

    sourceThd.join();
    sinkThd.join();

    watch.elapsed();
    System.out.println("Max Queue size " + channel.getMaxMemQueueSize() );
  }

  @Test
  public void testCounters() throws InterruptedException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity",  "5000");
    params.put("overflowCapacity","5000");
    params.put("transactionCapacity","5000");
    params.put("overflowTimeout", "0");
    startChannel(params);

    //1. fill up mem queue
    Thread sourceThd = makePutThread("src", 1, 5000, 2500, channel);
    sourceThd.start();
    sourceThd.join();

    Assert.assertEquals(5000, channel.getTotalStored());
    Assert.assertEquals(5000, channel.channelCounter.getChannelSize());
    Assert.assertEquals(5000, channel.channelCounter.getEventPutAttemptCount());
    Assert.assertEquals(5000, channel.channelCounter.getEventPutSuccessCount());

    //2. empty mem queue
    Thread sinkThd =  makeTakeThread("sink",  1, 5000, 1000, channel);
    sinkThd.start();
    sinkThd.join();
    Assert.assertEquals(0, channel.getTotalStored());
    Assert.assertEquals(0, channel.channelCounter.getChannelSize());
    Assert.assertEquals(5000, channel.channelCounter.getEventTakeAttemptCount());
    Assert.assertEquals(5000, channel.channelCounter.getEventTakeSuccessCount());


    //3. fill up mem & overflow
    sourceThd = makePutThread("src", 1, 10000, 1000, channel);
    sourceThd.start();
    sourceThd.join();
    Assert.assertEquals(10000, channel.getTotalStored());
    Assert.assertEquals(10000, channel.channelCounter.getChannelSize());
    Assert.assertEquals(15000, channel.channelCounter.getEventPutAttemptCount());
    Assert.assertEquals(15000, channel.channelCounter.getEventPutSuccessCount());


    //4. empty memory
    sinkThd = makeTakeThread("sink",  1, 5000, 1000, channel);
    sinkThd.start();
    sinkThd.join();
    Assert.assertEquals(5000, channel.getTotalStored());
    Assert.assertEquals(5000, channel.channelCounter.getChannelSize());
    Assert.assertEquals(10000, channel.channelCounter.getEventTakeAttemptCount());
    Assert.assertEquals(10000, channel.channelCounter.getEventTakeSuccessCount());

    //5. empty overflow
    transactionalTakeN(5001, 1000, channel);
    transactionalTakeN(6001, 1000, channel);
    transactionalTakeN(7001, 1000, channel);
    transactionalTakeN(8001, 1000, channel);
    transactionalTakeN(9001, 1000, channel);
    Assert.assertEquals(0, channel.getTotalStored());
    Assert.assertEquals(0, channel.channelCounter.getChannelSize());
    Assert.assertEquals(15000, channel.channelCounter.getEventTakeAttemptCount());
    Assert.assertEquals(15000, channel.channelCounter.getEventTakeSuccessCount());



    //6. now do it concurrently
    sourceThd = makePutThread("src1", 1, 5000, 1000, channel);
    Thread sourceThd2 = makePutThread("src2", 1, 5000, 500, channel);
    sinkThd =  makeTakeThread_noCheck("sink1", 5000, 1000, channel);
    sourceThd.start();
    sourceThd2.start();
    sinkThd.start();
    sourceThd.join();
    sourceThd2.join();
    sinkThd.join();
    Assert.assertEquals(5000, channel.getTotalStored());
    Assert.assertEquals(5000, channel.channelCounter.getChannelSize());
    Thread sinkThd2 =  makeTakeThread_noCheck("sink2", 2500, 500, channel);
    Thread sinkThd3 =  makeTakeThread_noCheck("sink3", 2500, 1000, channel);
    sinkThd2.start();
    sinkThd3.start();
    sinkThd2.join();
    sinkThd3.join();
    Assert.assertEquals(0, channel.getTotalStored());
    Assert.assertEquals(0, channel.channelCounter.getChannelSize());
    Assert.assertEquals(25000, channel.channelCounter.getEventTakeSuccessCount());
    Assert.assertEquals(25000, channel.channelCounter.getEventPutSuccessCount());
    Assert.assertTrue("TakeAttempt channel counter value larger than expected" ,
            25000 <= channel.channelCounter.getEventTakeAttemptCount());
    Assert.assertTrue("PutAttempt channel counter value larger than expected",
            25000 <= channel.channelCounter.getEventPutAttemptCount());
  }

  public ArrayList<Thread> createSourceThreads(int count, int totalEvents
          , int batchSize) {
    ArrayList<Thread> sourceThds = new ArrayList<Thread>();

    for (int i = 0; i < count; ++i) {
      sourceThds.add(  makePutThread("src" + i, 1, totalEvents/count
              , batchSize, channel) );
    }
    return sourceThds;
  }

  public ArrayList<Thread> createSinkThreads(int count, int totalEvents
          , int batchSize) {
    ArrayList<Thread> sinkThreads = new ArrayList<Thread>(count);

    for (int i = 0; i < count; ++i) {
      sinkThreads.add( makeTakeThread_noCheck("sink"+i, totalEvents/count
              , batchSize, channel) );
    }
    return sinkThreads;
  }

  public void startThreads(ArrayList<Thread> threads) {
    for (Thread thread : threads) {
      thread.start();
    }
  }

  public void joinThreads(ArrayList<Thread> threads)
          throws InterruptedException {
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        System.out.println("Interrupted while waiting on " + thread.getName() );
        throw e;
      }
    }
  }

  @Test
  public void testParallelMultipleSourcesAndSinks() throws InterruptedException {
    int sourceCount = 8;
    int sinkCount = 8;
    int eventCount = 1000000;
    int batchSize = 100;

    Map<String, String> params = new HashMap<String, String>();
    params.put("memoryCapacity", "0");
    params.put("overflowCapacity",  "500020");
    params.put("overflowTimeout", "3");
    startChannel(params);

    ArrayList<Thread> sinks = createSinkThreads(sinkCount, eventCount, batchSize);

    ArrayList<Thread> sources = createSourceThreads(sourceCount
            , eventCount, batchSize);


    StopWatch watch = new StopWatch();
    startThreads(sinks);
    startThreads(sources);

    joinThreads(sources);
    joinThreads(sinks);

    watch.elapsed();

    System.out.println("Total puts " + channel.drainOrder.totalPuts);

    System.out.println("Max Queue size " + channel.getMaxMemQueueSize() );
    System.out.println(channel.memQueue.size());

    System.out.println("done");
  }

  static class StopWatch {
    long startTime;

    public StopWatch() {
      startTime = System.currentTimeMillis();
    }

    public void elapsed() {
      elapsed(null);
    }

    public void elapsed(String suffix) {
      long elapsed = System.currentTimeMillis() - startTime;
      if (suffix == null) {
        suffix = "";
      } else {
        suffix = "{ " + suffix + " }";
      }

      if (elapsed < 10000) {
        System.out.println(Thread.currentThread().getName()
                +  " : [ " + elapsed + " ms ].        " + suffix);
      } else {
        System.out.println(Thread.currentThread().getName()
                +  " : [ " + elapsed / 1000 + " sec ].       " + suffix);
      }
    }
  }

}
