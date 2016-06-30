/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMemoryChannelConcurrency {

  private CyclicBarrier barrier;

  @Before
  public void setUp() {
  }

  @Test
  public void testTransactionConcurrency() throws InterruptedException {
    final Channel channel = new MemoryChannel();
    barrier = new CyclicBarrier(2);

    Configurables.configure(channel, new Context());
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        Transaction tx = channel.getTransaction();
        tx.begin();
        channel.put(EventBuilder.withBody("first event".getBytes()));
        try {
          barrier.await();
          barrier.await();
          tx.rollback();

          barrier.await();
          tx.close();
          // final barrier to make sure both threads manage to finish
          barrier.await();
        } catch (InterruptedException e) {
          Assert.fail();
        } catch (BrokenBarrierException e) {
          Assert.fail();
        }
      }
    });

    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        Transaction tx = channel.getTransaction();
        try {
          barrier.await();
          tx.begin();
          channel.put(EventBuilder.withBody("second event".getBytes()));
          barrier.await();
          barrier.await();
          tx.commit();
          tx.close();
          // final barrier to make sure both threads manage to finish
          barrier.await();
        } catch (InterruptedException e) {
          Assert.fail();
        } catch (BrokenBarrierException e) {
          Assert.fail();
        }
      }
    });
    t1.start();
    t2.start();

    t1.join(1000);
    if (t1.isAlive()) {
      Assert.fail("Thread1 failed to finish");
      t1.interrupt();
    }

    t2.join(1000);
    if (t2.isAlive()) {
      Assert.fail("Thread2 failed to finish");
      t2.interrupt();
    }

    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    Assert.assertEquals("second event", new String(e.getBody()));
    Assert.assertNull(channel.take());
    tx.commit();
    tx.close();
  }

  /**
   * Works with a startgate/endgate latches to make sure all threads run at the same time.
   * Threads randomly choose to commit or rollback random numbers of actions, tagging them with the
   * thread no. The correctness check is made by recording committed entries into a map, and
   * verifying the count after the endgate.
   * Since nothing is taking the puts out, allow for a big capacity
   *
   * @throws InterruptedException
   */
  @Test
  public void testManyThreads() throws InterruptedException {
    final Channel channel = new MemoryChannel();
    Context context = new Context();
    context.put("keep-alive", "1");
    context.put("capacity", "5000"); // theoretical maximum of 100 threads * 10 * 5
    // because we're just grabbing the whole lot in one commit
    // normally a transactionCapacity significantly lower than the channel capacity would be
    // recommended
    context.put("transactionCapacity", "5000");
    Configurables.configure(channel, context);
    final ConcurrentHashMap<String, AtomicInteger> committedPuts =
        new ConcurrentHashMap<String, AtomicInteger>();

    final int threadCount = 100;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch endGate = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          Long tid = Thread.currentThread().getId();
          String strtid = tid.toString();
          Random rng = new Random(tid);

          try {
            startGate.await();
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          for (int j = 0; j < 10; j++) {
            int events = rng.nextInt(5) + 1;
            Transaction tx = channel.getTransaction();
            tx.begin();
            for (int k = 0; k < events; k++) {
              channel.put(EventBuilder.withBody(strtid.getBytes()));
            }
            if (rng.nextBoolean()) {
              tx.commit();
              AtomicInteger tcount = committedPuts.get(strtid);
              if (tcount == null) {
                committedPuts.put(strtid, new AtomicInteger(events));
              } else {
                tcount.addAndGet(events);
              }
            } else {
              tx.rollback();
            }
            tx.close();
          }
          endGate.countDown();
        }
      };
      t.start();
    }
    startGate.countDown();
    endGate.await();

    if (committedPuts.isEmpty()) {
      Assert.fail();
    }

    // verify the counts
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e;
    while ((e = channel.take()) != null) {
      String index = new String(e.getBody());
      AtomicInteger remain = committedPuts.get(index);
      int post = remain.decrementAndGet();
      if (post == 0) {
        committedPuts.remove(index);
      }
    }
    tx.commit();
    tx.close();
    if (!committedPuts.isEmpty()) {
      Assert.fail();
    }
  }

  @Test
  public void testConcurrentSinksAndSources() throws InterruptedException {
    final Channel channel = new MemoryChannel();
    Context context = new Context();
    context.put("keep-alive", "1");
    context.put("capacity", "100"); // theoretical maximum of 100 threads * 10 * 5
    // because we're just grabbing the whole lot in one commit
    // normally a transactionCapacity significantly lower than the channel capacity would be
    // recommended
    context.put("transactionCapacity", "100");
    Configurables.configure(channel, context);
    final ConcurrentHashMap<String, AtomicInteger> committedPuts =
        new ConcurrentHashMap<String, AtomicInteger>();
    final ConcurrentHashMap<String, AtomicInteger> committedTakes =
        new ConcurrentHashMap<String, AtomicInteger>();

    final int threadCount = 100;
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch endGate = new CountDownLatch(threadCount);

    // start a sink and source for each
    for (int i = 0; i < threadCount / 2; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          Long tid = Thread.currentThread().getId();
          String strtid = tid.toString();
          Random rng = new Random(tid);

          try {
            startGate.await();
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          for (int j = 0; j < 10; j++) {
            int events = rng.nextInt(5) + 1;
            Transaction tx = channel.getTransaction();
            tx.begin();
            for (int k = 0; k < events; k++) {
              channel.put(EventBuilder.withBody(strtid.getBytes()));
            }
            if (rng.nextBoolean()) {
              try {
                tx.commit();
                AtomicInteger tcount = committedPuts.get(strtid);
                if (tcount == null) {
                  committedPuts.put(strtid, new AtomicInteger(events));
                } else {
                  tcount.addAndGet(events);
                }
              } catch (ChannelException e) {
                System.out.print("puts commit failed");
                tx.rollback();
              }
            } else {
              tx.rollback();
            }
            tx.close();
          }
          endGate.countDown();
        }
      };
      // start source
      t.start();
      final Integer takeMapLock = 0;
      t = new Thread() {
        @Override
        public void run() {
          Random rng = new Random(Thread.currentThread().getId());

          try {
            startGate.await();
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          for (int j = 0; j < 10; j++) {
            int events = rng.nextInt(5) + 1;
            Transaction tx = channel.getTransaction();
            tx.begin();
            Event[] taken = new Event[events];
            int k;
            for (k = 0; k < events; k++) {
              taken[k] = channel.take();
              if (taken[k] == null) break;
            }
            if (rng.nextBoolean()) {
              try {
                tx.commit();
                for (Event e : taken) {
                  if (e == null) break;
                  String index = new String(e.getBody());
                  synchronized (takeMapLock) {
                    AtomicInteger remain = committedTakes.get(index);
                    if (remain == null) {
                      committedTakes.put(index, new AtomicInteger(1));
                    } else {
                      remain.incrementAndGet();
                    }
                  }
                }
              } catch (ChannelException e) {
                System.out.print("takes commit failed");
                tx.rollback();
              }
            } else {
              tx.rollback();
            }
            tx.close();
          }
          endGate.countDown();
        }
      };
      // start sink
      t.start();
    }
    startGate.countDown();
    if (!endGate.await(20, TimeUnit.SECONDS)) {
      Assert.fail("Not all threads ended succesfully");
    }

    // verify the counts
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e;
    // first pull out what's left in the channel and remove it from the
    // committed map
    while ((e = channel.take()) != null) {
      String index = new String(e.getBody());
      AtomicInteger remain = committedPuts.get(index);
      int post = remain.decrementAndGet();
      if (post == 0) {
        committedPuts.remove(index);
      }
    }
    tx.commit();
    tx.close();

    // now just check the committed puts match the committed takes
    for (Entry<String, AtomicInteger> takes : committedTakes.entrySet()) {
      AtomicInteger count = committedPuts.get(takes.getKey());
      if (count == null) {
        Assert.fail("Putted data doesn't exist");
      }
      if (count.get() != takes.getValue().get()) {
        Assert.fail(String.format("Mismatched put and take counts expected %d had %d",
                                  count.get(), takes.getValue().get()));
      }
      committedPuts.remove(takes.getKey());
    }
    if (!committedPuts.isEmpty()) {
      Assert.fail("Puts still has entries remaining");
    }
  }
}
