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
package org.apache.flume.rpc.thrift;

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
import org.apache.flume.exception.ChannelException;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.rpc.thrift.client.ThriftRpcClient;
import org.apache.flume.rpc.thrift.source.ThriftSource;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.layered.TFastFramedTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.flume.util.Whitebox;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;

public class ThriftTestingSource {
  public final Queue<Event> flumeEvents = new ConcurrentLinkedQueue<Event>();
  private final TServer server;
  public int batchCount = 0;
  public int individualCount = 0;
  public int incompleteBatches = 0;
  private AtomicLong delay = null;

  public void setDelay(AtomicLong delay) {
    this.delay = delay;
  }

  private class ThriftOKHandler implements ThriftSourceProtocol.Iface {

    public ThriftOKHandler() {

    }

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      flumeEvents.add(EventBuilder.withBody(event.getBody(), event.getHeaders()));
      individualCount++;
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      batchCount++;
      if (events.size() < 10) {
        incompleteBatches++;
      }
      for (ThriftFlumeEvent event : events) {
        flumeEvents.add(EventBuilder.withBody(event.getBody(), event.getHeaders()));
      }
      return Status.OK;
    }
  }

  private class ThriftFailHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      return Status.FAILED;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws
                                                             TException {
      return Status.FAILED;
    }
  }

  private class ThriftErrorHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      throw new FlumeException("Forced Error");
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      throw new FlumeException("Forced Error");
    }
  }

  private class ThriftSlowHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(1550);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(1550);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  private class ThriftTimeoutHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(5000);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(5000);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  private class ThriftAlternateHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        if (delay != null) {
          TimeUnit.MILLISECONDS.sleep(delay.get());
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      try {
        if (delay != null) {
          TimeUnit.MILLISECONDS.sleep(delay.get());
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  private ThriftSourceProtocol.Iface getHandler(String handlerName) {
    ThriftSourceProtocol.Iface handler = null;
    if (handlerName.equals(HandlerType.OK.name())) {
      handler = new ThriftOKHandler();
    } else if (handlerName.equals(HandlerType.FAIL.name())) {
      handler = new ThriftFailHandler();
    } else if (handlerName.equals(HandlerType.ERROR.name())) {
      handler = new ThriftErrorHandler();
    } else if (handlerName.equals(HandlerType.SLOW.name())) {
      handler = new ThriftSlowHandler();
    } else if (handlerName.equals(HandlerType.TIMEOUT.name())) {
      handler = new ThriftTimeoutHandler();
    } else if (handlerName.equals(HandlerType.ALTERNATE.name())) {
      handler = new ThriftAlternateHandler();
    }
    return handler;
  }

  public ThriftTestingSource(String handlerName, int port, String protocol) throws Exception {
    TNonblockingServerTransport serverTransport =
        new TNonblockingServerSocket(new InetSocketAddress("0.0.0.0", port));
    ThriftSourceProtocol.Iface handler = getHandler(handlerName);

    TProtocolFactory transportProtocolFactory = null;
    if (protocol != null && protocol == ThriftRpcClient.BINARY_PROTOCOL) {
      transportProtocolFactory = new TBinaryProtocol.Factory();
    } else {
      transportProtocolFactory = new TCompactProtocol.Factory();
    }
    server = new THsHaServer(new THsHaServer.Args(serverTransport).processor(
        new ThriftSourceProtocol.Processor(handler)).protocolFactory(
            transportProtocolFactory));
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });
  }

  public ThriftTestingSource(String handlerName, int port,
                             String protocol, String keystore,
                             String keystorePassword, String keyManagerType,
                             String keystoreType) throws Exception {
    TSSLTransportFactory.TSSLTransportParameters params =
            new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keystore, keystorePassword, keyManagerType, keystoreType);

    TServerSocket serverTransport = TSSLTransportFactory.getServerSocket(
            port, 10000, InetAddress.getByName("0.0.0.0"), params);

    ThriftSourceProtocol.Iface handler = getHandler(handlerName);

    Class serverClass = Class.forName("org.apache.thrift" +
            ".server.TThreadPoolServer");
    Class argsClass = Class.forName("org.apache.thrift.server" +
            ".TThreadPoolServer$Args");
    TServer.AbstractServerArgs args = (TServer.AbstractServerArgs) argsClass
            .getConstructor(TServerTransport.class)
            .newInstance(serverTransport);
    Method m = argsClass.getDeclaredMethod("maxWorkerThreads", int.class);
    m.invoke(args, Integer.MAX_VALUE);
    TProtocolFactory transportProtocolFactory = null;
    if (protocol != null && protocol == ThriftRpcClient.BINARY_PROTOCOL) {
      transportProtocolFactory = new TBinaryProtocol.Factory();
    } else {
      transportProtocolFactory = new TCompactProtocol.Factory();
    }
    args.protocolFactory(transportProtocolFactory);
    args.inputTransportFactory(new TFastFramedTransport.Factory());
    args.outputTransportFactory(new TFastFramedTransport.Factory());
    args.processor(new ThriftSourceProtocol.Processor<ThriftSourceProtocol.Iface>(handler));
    server = (TServer) serverClass.getConstructor(argsClass).newInstance(args);
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });
  }

  public enum HandlerType {
    OK,
    FAIL,
    ERROR,
    SLOW,
    TIMEOUT,
    ALTERNATE;
  }

  public void stop() {
    server.stop();
  }

    public static class TestThriftSource {

      private ThriftSource source;
      private MemoryChannel channel;
      private RpcClient client;
      private final Properties props = new Properties();
      private int port;

      @Before
      public void setUp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
          port = socket.getLocalPort();
        }
        props.clear();
        props.setProperty("hosts", "h1");
        props.setProperty("hosts.h1", "0.0.0.0:" + String.valueOf(port));
        props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, "10");
        props.setProperty(RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT, "2000");
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

      @Ignore("This test is flakey and causes tests to fail pretty often.")
      @Test
      public void testAppendSSLWithComponentKeystore() throws Exception {

        Context context = new Context();
        channel.configure(context);
        configureSource();

        context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
        context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
        context.put("ssl", "true");
        context.put("keystore", "src/test/resources/keystorefile.jks");
        context.put("keystore-password", "password");
        context.put("keystore-type", "JKS");

        Configurables.configure(source, context);

        doAppendSSL();
      }

      @Ignore("This test is flakey and causes tests to fail pretty often.")
      @Test
      public void testAppendSSLWithGlobalKeystore() throws Exception {

        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/keystorefile.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");

        Context context = new Context();
        channel.configure(context);
        configureSource();

        context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
        context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
        context.put("ssl", "true");

        Configurables.configure(source, context);

        doAppendSSL();

        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.keyStoreType");
      }

      private void doAppendSSL() throws EventDeliveryException {
        Properties sslprops = (Properties)props.clone();
        sslprops.put("ssl", "true");
        sslprops.put("truststore", "src/test/resources/truststorefile.jks");
        sslprops.put("truststore-password", "password");
        client = RpcClientFactory.getThriftInstance(sslprops);

        source.start();
        for (int i = 0; i < 30; i++) {
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
      public void testAppend() throws Exception {
        client = RpcClientFactory.getThriftInstance(props);
        Context context = new Context();
        channel.configure(context);
        configureSource();
        context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
        context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
        Configurables.configure(source, context);
        source.start();
        for (int i = 0; i < 30; i++) {
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
          Assert.assertTrue(Long.valueOf(event.getHeaders().get("time")) <= after);
          events.add(Integer.parseInt(new String(event.getBody())));
        }
        transaction.commit();
        transaction.close();

        Collections.sort(events);

        int index = 0;
        //30 batches of 10
        for (int i = 0; i < 30; i++) {
          for (int j = 0; j < 10; j++) {
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
        for (int i = 0; i < 5; i++) {
          for (int j = 0; j < 500; j++) {
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
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(submitter);
        for (int i = 0; i < 30; i++) {
          completionService.submit(new SubmitHelper(i), null);
        }
        //wait for all threads to be done


        for (int i = 0; i < 30; i++) {
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
        for (int i = 0; i < 30; i++) {
          for (int j = 0; j < 10; j++) {
            Assert.assertEquals(i, events.get(index++).intValue());
          }
        }
      }

      @Test
      public void testErrorCounterChannelWriteFail() throws Exception {
        client = RpcClientFactory.getThriftInstance(props);
        Context context = new Context();
        context.put(ThriftSource.CONFIG_BIND, "0.0.0.0");
        context.put(ThriftSource.CONFIG_PORT, String.valueOf(port));
        source.configure(context);
        ChannelProcessor cp = Mockito.mock(ChannelProcessor.class);
        doThrow(new ChannelException("dummy")).when(cp).processEvent(any(Event.class));
        doThrow(new ChannelException("dummy")).when(cp).processEventBatch(anyList());
        source.setChannelProcessor(cp);
        source.start();
        Event event = EventBuilder.withBody("hello".getBytes());
        try {
          client.append(event);
        } catch (EventDeliveryException e) {
          //
        }
        try {
          client.appendBatch(Arrays.asList(event));
        } catch (EventDeliveryException e) {
          //
        }
        SourceCounter sc = (SourceCounter) Whitebox.getInternalState(source, "sourceCounter");
        Assert.assertEquals(2, sc.getChannelWriteFail());
        source.stop();
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
}
