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

import java.util.List;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

public class TestChannelUtils
    extends AbstractBasicChannelSemanticsTest {

  @Test
  public void testHappyPath1() {
    ChannelUtils.put(channel, events.get(0));
    Assert.assertTrue(channel.wasLastTransactionCommitted());
    Assert.assertFalse(channel.wasLastTransactionRolledBack());
    Assert.assertTrue(channel.wasLastTransactionClosed());
  }

  @Test
  public void testHappyPath2() {
    ChannelUtils.take(channel);
    Assert.assertTrue(channel.wasLastTransactionCommitted());
    Assert.assertFalse(channel.wasLastTransactionRolledBack());
    Assert.assertTrue(channel.wasLastTransactionClosed());
  }

  @Test
  public void testHappyPath3() {
    ChannelUtils.put(channel, events.get(0));
    Assert.assertSame(events.get(0), ChannelUtils.take(channel));
  }

  @Test
  public void testHappyPath4() {
    for (int i = 0; i < events.size(); ++i) {
      ChannelUtils.put(channel, events.get(i));
    }
    for (int i = 0; i < events.size(); ++i) {
      Assert.assertSame(events.get(i), ChannelUtils.take(channel));
    }
  }

  @Test
  public void testHappyPath5() {
    int rounds = 10;
    for (int i = 0; i < rounds; ++i) {
      ChannelUtils.put(channel, events);
    }
    for (int i = 0; i < rounds; ++i) {
      List<Event> takenEvents = ChannelUtils.take(channel, events.size());
      Assert.assertTrue(takenEvents.size() == events.size());
      for (int j = 0; j < events.size(); ++j) {
        Assert.assertSame(events.get(j), takenEvents.get(j));
      }
    }
  }

  private void testTransact(final TestChannel.Mode mode,
      Class<? extends Throwable> exceptionClass, final Runnable test) {
    testException(exceptionClass, new Runnable() {
        @Override
        public void run() {
          ChannelUtils.transact(channel, new Runnable() {
              @Override
              public void run() {
                testMode(mode, test);
              }
            });
        }
      });
    Assert.assertFalse(channel.wasLastTransactionCommitted());
    Assert.assertTrue(channel.wasLastTransactionRolledBack());
    Assert.assertTrue(channel.wasLastTransactionClosed());
  }

  private void testTransact(TestChannel.Mode mode,
      Class<? extends Throwable> exceptionClass) {
    testTransact(mode, exceptionClass, new Runnable() {
        @Override
        public void run() {
          channel.put(events.get(0));
        }
      });
  }

  @Test
  public void testError() {
    testTransact(TestChannel.Mode.THROW_ERROR, TestError.class);
  }

  @Test
  public void testRuntimeException() {
    testTransact(TestChannel.Mode.THROW_RUNTIME, TestRuntimeException.class);
  }

  @Test
  public void testChannelException() {
    testTransact(TestChannel.Mode.THROW_CHANNEL, ChannelException.class);
  }

  @Test
  public void testInterrupt() throws Exception {
    testTransact(TestChannel.Mode.SLEEP, InterruptedException.class,
        new Runnable() {
          @Override
          public void run() {
            interruptTest(new Runnable() {
                @Override
                public void run() {
                  channel.put(events.get(0));
                }
              });
          }
      });
  }
}
