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

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestSequenceGeneratorSource {

  private PollableSource source;

  @Before
  public void setUp() {
    source = new SequenceGeneratorSource();
  }

  @Test
  public void testProcess() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context();

    context.put("logicalNode.name", "test");

    Configurables.configure(source, context);
    Configurables.configure(channel, context);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    source.start();

    for (long i = 0; i < 100; i++) {
      source.process();
      Event event = channel.take();

      Assert.assertArrayEquals(String.valueOf(i).getBytes(),
          new String(event.getBody()).getBytes());
    }
  }

  @Test
  public void testBatchProcessWithLifeCycle() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    int batchSize = 10;

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context();

    context.put("logicalNode.name", "test");
    context.put("batchSize", Integer.toString(batchSize));

    Configurables.configure(source, context);
    Configurables.configure(channel, context);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();

    for (long i = 0; i < 100; i++) {
      source.process();

      for (long j = batchSize; j > 0; j--) {
        Event event = channel.take();
        String expectedVal = String.valueOf(((i + 1) * batchSize) - j);
        String resultedVal = new String(event.getBody());
        Assert.assertTrue("Expected " + expectedVal + " is not equals to " +
            resultedVal, expectedVal.equals(resultedVal));
      }
    }

    source.stop();
  }

  @Test
  public void testLifecycle() throws InterruptedException,
      EventDeliveryException {

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context();

    context.put("logicalNode.name", "test");

    Configurables.configure(source, context);
    Configurables.configure(channel, context);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();

    for (long i = 0; i < 100; i++) {
      source.process();
      Event event = channel.take();

      Assert.assertArrayEquals(String.valueOf(i).getBytes(),
          new String(event.getBody()).getBytes());
    }
    source.stop();
  }
}
