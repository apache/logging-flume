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

import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNetcatSource {

  private Channel channel;
  private EventDrivenSource source;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
    source = new NetcatSource();

    Context context = new Context();

    Configurables.configure(channel, context);
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    ExecutorService executor = Executors.newFixedThreadPool(3);
    Context context = new Context();

    /* FIXME: Use a random port for testing. */
    context.put("bind", "0.0.0.0");
    context.put("port", "41414");

    Configurables.configure(source, context);

    source.start();

    /* FIXME: Ensure proper send / received semantics. */

    Runnable clientRequestRunnable = new Runnable() {

      @Override
      public void run() {
        try {
          SocketChannel clientChannel = SocketChannel
              .open(new InetSocketAddress(41414));

          Writer writer = Channels.newWriter(clientChannel, "utf-8");

          writer.write("Test message");

          writer.flush();
          clientChannel.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    };

    ChannelSelector seclector = source.getChannelProcessor().getSelector();
    Transaction tx = seclector.getAllChannels().get(0).getTransaction();
    tx.begin();

    for (int i = 0; i < 100; i++) {
      executor.submit(clientRequestRunnable);

      Event event = channel.take();

      Assert.assertNotNull(event);
      Assert.assertArrayEquals("Test message".getBytes(), event.getBody());
    }

    tx.commit();
    tx.close();
    executor.shutdown();

    while (!executor.isTerminated()) {
      executor.awaitTermination(500, TimeUnit.MILLISECONDS);
    }

    source.stop();
  }

}
