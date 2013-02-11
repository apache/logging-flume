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
import com.google.common.collect.Maps;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestThriftSource {

  private ThriftSource source;
  private MemoryChannel channel;
  private RpcClient client;
  private final Random random = new Random();
  private final Properties props = new Properties();
  private int port;

  @Before
  public void setUp() {
    port = random.nextInt(50000) + 1024;
    props.clear();
    props.setProperty("hosts", "h1");
    props.setProperty("hosts.h1", "0.0.0.0:"+ String.valueOf(port));
    props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "10");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT,
      "2000");
    channel = new MemoryChannel();
    source = new ThriftSource();
  }

  @After
  public void stop() throws Exception {
    source.stop();
  }

  private void configureSource() {
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }


  @Test
  public void testAppend() throws Exception {
    client = RpcClientFactory.getThriftInstance(props);
    Context context = new Context();
    channel.configure(context);
    configureSource();
    context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
    context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
    Configurables.configure(source, context);
    source.start();
    for(int i = 0; i < 30; i++) {
      client.append(EventBuilder.withBody(String.valueOf(i).getBytes()));
    }
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    for (int i = 0; i < 30; i++) {
      Event event = channel.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(String.valueOf(i), new String(event.getBody()));
    }
    transaction.commit();
    transaction.close();
  }

  @Test
  public void testAppendBatch() throws Exception {
    client = RpcClientFactory.getThriftInstance(props);
    Context context = new Context();
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    channel.configure(context);
    configureSource();
    context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
    context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
    Configurables.configure(source, context);
    source.start();
    for (int i = 0; i < 30; i++) {
      List<Event> events = Lists.newArrayList();
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = Maps.newHashMap();
        hdrs.put("time", String.valueOf(System.currentTimeMillis()));
        events.add(EventBuilder.withBody(String.valueOf(i).getBytes(), hdrs));
      }
      client.appendBatch(events);
    }
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    long after = System.currentTimeMillis();
    List<Integer> events = Lists.newArrayList();
    for (int i = 0; i < 300; i++) {
      Event event = channel.take();
      Assert.assertNotNull(event);
      Assert.assertTrue(Long.valueOf(event.getHeaders().get("time")) < after);
      events.add(Integer.parseInt(new String(event.getBody())));
    }
    transaction.commit();
    transaction.close();

    Collections.sort(events);

    int index = 0;
    //30 batches of 10
    for(int i = 0; i < 30; i++) {
      for(int j = 0; j < 10; j++) {
        Assert.assertEquals(i, events.get(index++).intValue());
      }
    }
  }

  @Test
  public void testAppendBigBatch() throws Exception {
    client = RpcClientFactory.getThriftInstance(props);
    Context context = new Context();
    context.put("capacity", "3000");
    context.put("transactionCapacity", "3000");
    channel.configure(context);
    configureSource();
    context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
    context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
    Configurables.configure(source, context);
    source.start();
    for (int i = 0; i < 5; i++) {
      List<Event> events = Lists.newArrayList();
      for (int j = 0; j < 500; j++) {
        Map<String, String> hdrs = Maps.newHashMap();
        hdrs.put("time", String.valueOf(System.currentTimeMillis()));
        events.add(EventBuilder.withBody(String.valueOf(i).getBytes(), hdrs));
      }
      client.appendBatch(events);
    }
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    long after = System.currentTimeMillis();
    List<Integer> events = Lists.newArrayList();
    for (int i = 0; i < 2500; i++) {
      Event event = channel.take();
      Assert.assertNotNull(event);
      Assert.assertTrue(Long.valueOf(event.getHeaders().get("time")) < after);
      events.add(Integer.parseInt(new String(event.getBody())));
    }
    transaction.commit();
    transaction.close();

    Collections.sort(events);

    int index = 0;
    //10 batches of 500
    for(int i = 0; i < 5; i++) {
      for(int j = 0; j < 500; j++) {
        Assert.assertEquals(i, events.get(index++).intValue());
      }
    }
  }

  @Test
  public void testMultipleClients() throws Exception {
    ExecutorService submitter = Executors.newCachedThreadPool();
    client = RpcClientFactory.getThriftInstance(props);
    Context context = new Context();
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    channel.configure(context);
    configureSource();
    context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
    context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
    Configurables.configure(source, context);
    source.start();
    ExecutorCompletionService<Void> completionService = new
      ExecutorCompletionService(submitter);
    for (int i = 0; i < 30; i++) {
      completionService.submit(new SubmitHelper(i), null);
    }
    //wait for all threads to be done


    for(int i = 0; i < 30; i++) {
      completionService.take();
    }

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    long after = System.currentTimeMillis();
    List<Integer> events = Lists.newArrayList();
    for (int i = 0; i < 300; i++) {
      Event event = channel.take();
      Assert.assertNotNull(event);
      Assert.assertTrue(Long.valueOf(event.getHeaders().get("time")) < after);
      events.add(Integer.parseInt(new String(event.getBody())));
    }
    transaction.commit();
    transaction.close();

    Collections.sort(events);

    int index = 0;
    //30 batches of 10
    for(int i = 0; i < 30; i++) {
      for(int j = 0; j < 10; j++) {
        Assert.assertEquals(i, events.get(index++).intValue());
      }
    }
  }

  private class SubmitHelper implements Runnable {

    private final int i;
    public SubmitHelper(int i) {
      this.i = i;
    }
    @Override
    public void run() {
      List<Event> events = Lists.newArrayList();
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = Maps.newHashMap();
        hdrs.put("time", String.valueOf(System.currentTimeMillis()));
        events.add(EventBuilder.withBody(String.valueOf(i).getBytes(), hdrs));
      }
      try {
        client.appendBatch(events);
      } catch (EventDeliveryException e) {
        throw new FlumeException(e);
      }
    }
  }
}
