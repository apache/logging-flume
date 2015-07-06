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

package org.apache.flume.source;

import com.google.common.collect.Lists;
import java.util.concurrent.CountDownLatch;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ChannelSelectorFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPollableSourceRunner {

  private static final Logger logger = LoggerFactory
      .getLogger(TestPollableSourceRunner.class);

  private PollableSourceRunner sourceRunner;

  @Before
  public void setUp() {
    sourceRunner = new PollableSourceRunner();
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    final Channel channel = new MemoryChannel();
    final CountDownLatch latch = new CountDownLatch(50);

    Configurables.configure(channel, new Context());

    final ChannelSelector cs = new ReplicatingChannelSelector();
    cs.setChannels(Lists.newArrayList(channel));

    PollableSource source = new PollableSource() {

      private String name;
      private ChannelProcessor cp = new ChannelProcessor(cs);

      @Override
      public Status process() throws EventDeliveryException {
        Transaction transaction = channel.getTransaction();

        try {
          transaction.begin();
          Event event = EventBuilder.withBody(String.valueOf(
              "Event " + latch.getCount()).getBytes());

          latch.countDown();

          if (latch.getCount() % 20 == 0) {
            throw new EventDeliveryException("I don't like event:" + event);
          }
          channel.put(event);
          transaction.commit();
          return Status.READY;
        } catch (EventDeliveryException e) {
          logger.error("Unable to deliver event. Exception follows.", e);
          transaction.rollback();
          return Status.BACKOFF;
        } finally {
          transaction.close();
        }
      }

      @Override
      public long getBackOffSleepIncrement() {
        return PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT;
      }

      @Override
      public long getMaxBackOffSleepInterval() {
        return PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP;
      }

      @Override
      public void start() {
        // Unused.
      }

      @Override
      public void stop() {
        // Unused.
      }

      @Override
      public LifecycleState getLifecycleState() {
        // Unused.
        return null;
      }

      @Override
      public void setName(String name) {
        this.name = name;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public void setChannelProcessor(ChannelProcessor channelProcessor) {
        cp = channelProcessor;
      }

      @Override
      public ChannelProcessor getChannelProcessor() {
        return cp;
      }

    };

    sourceRunner.setSource(source);
    sourceRunner.start();

    latch.await();

    sourceRunner.stop();
  }

}
