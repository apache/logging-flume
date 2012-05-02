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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.base.Preconditions;

public abstract class AbstractBasicChannelSemanticsTest {

  protected static List<Event> events;
  static {
    Event[] array = new Event[7];
    for (int i = 0; i < array.length; ++i) {
      array[i] = EventBuilder.withBody(("test event " + i).getBytes());
    }
    events = Collections.unmodifiableList(Arrays.asList(array));
  }

  protected ExecutorService executor = null;
  protected TestChannel channel = null;

  protected static class TestChannel extends BasicChannelSemantics {

    private Queue<Event> queue = new ArrayDeque<Event>();

    public enum Mode {
      NORMAL,
      THROW_ERROR,
      THROW_RUNTIME,
      THROW_CHANNEL,
      SLEEP
    };

    private Mode mode = Mode.NORMAL;
    private boolean lastTransactionCommitted = false;
    private boolean lastTransactionRolledBack = false;
    private boolean lastTransactionClosed = false;

    public Mode getMode() {
      return mode;
    }

    public void setMode(Mode mode) {
      this.mode = mode;
    }

    public boolean wasLastTransactionCommitted() {
      return lastTransactionCommitted;
    }

    public boolean wasLastTransactionRolledBack() {
      return lastTransactionRolledBack;
    }

    public boolean wasLastTransactionClosed() {
      return lastTransactionClosed;
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
      return new TestTransaction();
    }

    protected class TestTransaction extends BasicTransactionSemantics {

      protected void doMode() throws InterruptedException {
        switch (mode) {
          case THROW_ERROR:
            throw new TestError();
          case THROW_RUNTIME:
            throw new TestRuntimeException();
          case THROW_CHANNEL:
            throw new ChannelException("test");
          case SLEEP:
            Thread.sleep(300000);
            break;
        }
      }

      @Override
      protected void doBegin() throws InterruptedException {
        doMode();
      }

      @Override
      protected void doPut(Event event) throws InterruptedException {
        doMode();
        synchronized (queue) {
          queue.add(event);
        }
      }

      @Override
      protected Event doTake() throws InterruptedException {
        doMode();
        synchronized (queue) {
          return queue.poll();
        }
      }

      @Override
      protected void doCommit() throws InterruptedException {
        doMode();
        lastTransactionCommitted = true;
      }

      @Override
      protected void doRollback() throws InterruptedException {
        lastTransactionRolledBack = true;
        doMode();
      }

      @Override
      protected void doClose() {
        lastTransactionClosed = true;
        Preconditions.checkState(mode != TestChannel.Mode.SLEEP,
            "doClose() can't throw InterruptedException, so why SLEEP?");
        try {
          doMode();
        } catch (InterruptedException e) {
          Assert.fail();
        }
      }
    }
  }

  protected static class TestError extends Error {
    static final long serialVersionUID = -1;
  };

  protected static class TestRuntimeException extends RuntimeException {
    static final long serialVersionUID = -1;
  };

  protected void testException(Class<? extends Throwable> exceptionClass,
      Runnable test) {
    try {
      test.run();
      Assert.fail();
    } catch (Throwable e) {
      if (exceptionClass == InterruptedException.class
          && e instanceof ChannelException
          && e.getCause() instanceof InterruptedException) {
        Assert.assertTrue(Thread.interrupted());
      } else if (!exceptionClass.isInstance(e)) {
        throw new AssertionError(e);
      }
    }
  }

  protected void testIllegalArgument(Runnable test) {
    testException(IllegalArgumentException.class, test);
  }

  protected void testIllegalState(Runnable test) {
    testException(IllegalStateException.class, test);
  }

  protected void testWrongThread(final Runnable test) throws Exception {
    executor.submit(new Runnable() {
        @Override
        public void run() {
          testIllegalState(test);
        }
      }).get();
  }

  protected void testMode(TestChannel.Mode mode, Runnable test) {
    TestChannel.Mode oldMode = channel.getMode();
    try {
      channel.setMode(mode);
      test.run();
    } finally {
      channel.setMode(oldMode);
    }
  }

  protected void testException(TestChannel.Mode mode,
      final Class<? extends Throwable> exceptionClass, final Runnable test) {
    testMode(mode, new Runnable() {
        @Override
        public void run() {
          testException(exceptionClass, test);
        }
      });
  }

  protected void testError(Runnable test) {
    testException(TestChannel.Mode.THROW_ERROR, TestError.class, test);
  }

  protected void testRuntimeException(Runnable test) {
    testException(TestChannel.Mode.THROW_RUNTIME, TestRuntimeException.class,
        test);
  }

  protected void testChannelException(Runnable test) {
    testException(TestChannel.Mode.THROW_CHANNEL, ChannelException.class, test);
  }

  protected void testInterrupt(final Runnable test) {
    testMode(TestChannel.Mode.SLEEP, new Runnable() {
        @Override
        public void run() {
          testException(InterruptedException.class, new Runnable() {
              @Override
              public void run() {
                interruptTest(test);
              }
            });
        }
      });
  }

  protected void interruptTest(final Runnable test) {
    final Thread mainThread = Thread.currentThread();
    Future<?> future = executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
          }
          mainThread.interrupt();
        }
      });
    test.run();
    try {
      future.get();
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  protected void testExceptions(Runnable test) throws Exception {
    testWrongThread(test);
    testBasicExceptions(test);
    testInterrupt(test);
  }

  protected void testBasicExceptions(Runnable test) throws Exception {
    testError(test);
    testRuntimeException(test);
    testChannelException(test);
  }

  @Before
  public void before() {
    Preconditions.checkState(channel == null, "test cleanup failed!");
    Preconditions.checkState(executor == null, "test cleanup failed!");
    channel = new TestChannel();
    executor = Executors.newCachedThreadPool();
  }

  @After
  public void after() {
    channel = null;
    executor.shutdown();
    executor = null;
  }
}
