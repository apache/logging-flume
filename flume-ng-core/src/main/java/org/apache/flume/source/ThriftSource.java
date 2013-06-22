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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.thrift.Status;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ThriftSource extends AbstractSource implements Configurable,
  EventDrivenSource {

  public static final Logger logger = LoggerFactory.getLogger(ThriftSource
    .class);
  /**
   * Config param for the maximum number of threads this source should use to
   * handle incoming data.
   */
  public static final String CONFIG_THREADS = "threads";
  /**
   * Config param for the hostname to listen on.
   */
  public static final String CONFIG_BIND = "bind";
  /**
   * Config param for the port to listen on.
   */
  public static final String CONFIG_PORT = "port";
  private Integer port;
  private String bindAddress;
  private int maxThreads = 0;
  private SourceCounter sourceCounter;
  private TServer server;
  private TServerTransport serverTransport;
  private ExecutorService servingExecutor;

  @Override
  public void configure(Context context) {
    logger.info("Configuring thrift source.");
    port = context.getInteger(CONFIG_PORT);
    Preconditions.checkNotNull(port, "Port must be specified for Thrift " +
      "Source.");
    bindAddress = context.getString(CONFIG_BIND);
    Preconditions.checkNotNull(bindAddress, "Bind address must be specified " +
      "for Thrift Source.");

    try {
      maxThreads = context.getInteger(CONFIG_THREADS, 0);
    } catch (NumberFormatException e) {
      logger.warn("Thrift source\'s \"threads\" property must specify an " +
        "integer value: " + context.getString(CONFIG_THREADS));
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting thrift source");

    maxThreads = (maxThreads <= 0) ? Integer.MAX_VALUE : maxThreads;
    Class<?> serverClass = null;
    Class<?> argsClass = null;
    TServer.AbstractServerArgs args = null;
    /*
     * Use reflection to determine if TThreadedSelectServer is available. If
     * it is not available, use TThreadPoolServer
     */
    try {
      serverClass = Class.forName("org.apache.thrift" +
        ".server.TThreadedSelectorServer");

      argsClass = Class.forName("org.apache.thrift" +
        ".server.TThreadedSelectorServer$Args");

      // Looks like TThreadedSelectorServer is available, so continue..
      ExecutorService sourceService;
      ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
        "Flume Thrift IPC Thread %d").build();
      if (maxThreads == 0) {
        sourceService = Executors.newCachedThreadPool(threadFactory);
      } else {
        sourceService = Executors.newFixedThreadPool(maxThreads, threadFactory);
      }
      serverTransport = new TNonblockingServerSocket(
        new InetSocketAddress(bindAddress, port));
      args = (TNonblockingServer.AbstractNonblockingServerArgs) argsClass
        .getConstructor(TNonblockingServerTransport.class)
        .newInstance(serverTransport);
      Method m = argsClass.getDeclaredMethod("executorService",
        ExecutorService.class);
      m.invoke(args, sourceService);
    } catch (ClassNotFoundException e) {
      logger.info("TThreadedSelectorServer not found, " +
        "using TThreadPoolServer");
      try {
        // Looks like TThreadedSelectorServer is not available,
        // so create a TThreadPoolServer instead.

        serverTransport = new TServerSocket(new InetSocketAddress
          (bindAddress, port));

        serverClass = Class.forName("org.apache.thrift" +
          ".server.TThreadPoolServer");
        argsClass = Class.forName("org.apache.thrift.server" +
          ".TThreadPoolServer$Args");
        args = (TServer.AbstractServerArgs) argsClass
          .getConstructor(TServerTransport.class)
          .newInstance(serverTransport);
        Method m = argsClass.getDeclaredMethod("maxWorkerThreads",int.class);
        m.invoke(args, maxThreads);
      } catch (ClassNotFoundException e1) {
        throw new FlumeException("Cannot find TThreadSelectorServer or " +
          "TThreadPoolServer. Please install a compatible version of thrift " +
          "in the classpath", e1);
      } catch (Throwable throwable) {
        throw new FlumeException("Cannot start Thrift source.", throwable);
      }
    } catch (Throwable throwable) {
      throw new FlumeException("Cannot start Thrift source.", throwable);
    }

    try {

      args.protocolFactory(new TCompactProtocol.Factory());
      args.inputTransportFactory(new TFastFramedTransport.Factory());
      args.outputTransportFactory(new TFastFramedTransport.Factory());
      args.processor(new ThriftSourceProtocol
        .Processor<ThriftSourceHandler>(new ThriftSourceHandler()));
      /*
       * Both THsHaServer and TThreadedSelectorServer allows us to pass in
       * the executor service to use - unfortunately the "executorService"
       * method does not exist in the parent abstract Args class,
       * so use reflection to pass the executor in.
       *
       */

      server = (TServer) serverClass.getConstructor(argsClass).newInstance
        (args);
    } catch (Throwable ex) {
      throw new FlumeException("Cannot start Thrift Source.", ex);
    }


    servingExecutor = Executors.newSingleThreadExecutor(new
      ThreadFactoryBuilder().setNameFormat("Flume Thrift Source I/O Boss")
      .build());

    /**
     * Start serving.
     */
    servingExecutor.submit(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });

    long timeAfterStart = System.currentTimeMillis();
    while(!server.isServing()) {
      try {
        if(System.currentTimeMillis() - timeAfterStart >=10000) {
          throw new FlumeException("Thrift server failed to start!");
        }
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FlumeException("Interrupted while waiting for Thrift server" +
          " to start.", e);
      }
    }
    sourceCounter.start();
    logger.info("Started Thrift source.");
    super.start();
  }

  public void stop() {
    if(server != null && server.isServing()) {
      server.stop();
    }
    if (servingExecutor != null) {
      servingExecutor.shutdown();
      try {
        if (!servingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          servingExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Interrupted while waiting for server to be " +
          "shutdown.");
      }
    }
    sourceCounter.stop();
    super.stop();
  }

  private class ThriftSourceHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      Event flumeEvent = EventBuilder.withBody(event.getBody(),
        event.getHeaders());

      sourceCounter.incrementAppendReceivedCount();
      sourceCounter.incrementEventReceivedCount();

      try {
        getChannelProcessor().processEvent(flumeEvent);
      } catch (ChannelException ex) {
        logger.warn("Thrift source " + getName() + " could not append events " +
          "to the channel.", ex);
        return Status.FAILED;
      }
      sourceCounter.incrementAppendAcceptedCount();
      sourceCounter.incrementEventAcceptedCount();
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      sourceCounter.incrementAppendBatchReceivedCount();
      sourceCounter.addToEventReceivedCount(events.size());

      List<Event> flumeEvents = Lists.newArrayList();
      for(ThriftFlumeEvent event : events) {
        flumeEvents.add(EventBuilder.withBody(event.getBody(),
          event.getHeaders()));
      }

      try {
        getChannelProcessor().processEventBatch(flumeEvents);
      } catch (ChannelException ex) {
        logger.warn("Thrift source %s could not append events to the " +
          "channel.", getName());
        return Status.FAILED;
      }

      sourceCounter.incrementAppendBatchAcceptedCount();
      sourceCounter.addToEventAcceptedCount(events.size());
      return Status.OK;
    }
  }

}
