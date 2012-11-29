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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Collection;
import java.util.Arrays;

import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class TestNetcatSource {

  private Channel channel;
  private EventDrivenSource source;
  private boolean ackEveryEvent;

  private static final Logger logger =
      LoggerFactory.getLogger(TestNetcatSource.class);

  public TestNetcatSource(boolean ackForEveryEvent) {
    ackEveryEvent = ackForEveryEvent;
  }

  @Parameters
  public static Collection data() {
    Object[][] data = new Object[][] { { true }, { false } };
   return Arrays.asList(data);
  }

  @Before
  public void setUp() {
    logger.info("Running setup");

    channel = new MemoryChannel();
    source = new NetcatSource();

    Context context = new Context();

    Configurables.configure(channel, context);
    List<Channel> channels = Lists.newArrayList(channel);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    ExecutorService executor = Executors.newFixedThreadPool(3);
    boolean bound = false;

    for(int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();
        context.put("bind", "0.0.0.0");
        context.put("port", "41414");
        context.put("ack-every-event", String.valueOf(ackEveryEvent));

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (FlumeException e) {
        // assume port in use, try another one
      }
    }

    Runnable clientRequestRunnable = new Runnable() {

      @Override
      public void run() {
        try {
          SocketChannel clientChannel = SocketChannel
              .open(new InetSocketAddress(41414));

          Writer writer = Channels.newWriter(clientChannel, "utf-8");
          BufferedReader reader = new BufferedReader(
              Channels.newReader(clientChannel, "utf-8"));

          writer.write("Test message\n");
          writer.flush();

          if (ackEveryEvent) {
                String response = reader.readLine();
          	Assert.assertEquals("Server should return OK", "OK", response);
          } else {
                Assert.assertFalse("Server should not return anything", reader.ready());
          }
          clientChannel.close();
        } catch (IOException e) {
          logger.error("Caught exception: ", e);
        }
      }

    };

    ChannelSelector selector = source.getChannelProcessor().getSelector();
    Transaction tx = selector.getAllChannels().get(0).getTransaction();
    tx.begin();

    for (int i = 0; i < 100; i++) {
      logger.info("Sending request");

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
