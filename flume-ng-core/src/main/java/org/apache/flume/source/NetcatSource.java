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
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A netcat-like source that listens on a given port and turns each line of text
 * into an event.
 * </p>
 * <p>
 * This source, primarily built for testing and exceedingly simple systems, acts
 * like <tt>nc -k -l [host] [port]</tt>. In other words, it opens a specified
 * port and listens for data. The expectation is that the supplied data is
 * newline separated text. Each line of text is turned into a Flume event and
 * sent via the connected channel.
 * </p>
 * <p>
 * Most testing has been done by using the <tt>nc</tt> client but other,
 * similarly implemented, clients should work just fine.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class NetcatSource extends AbstractSource implements Configurable,
    EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(NetcatSource.class);

  private String hostName;
  private int port;

  private CounterGroup counterGroup;
  private ServerSocketChannel serverSocket;
  private AtomicBoolean acceptThreadShouldStop;
  private Thread acceptThread;
  private ExecutorService handlerService;

  public NetcatSource() {
    super();

    port = 0;
    counterGroup = new CounterGroup();
    acceptThreadShouldStop = new AtomicBoolean();
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, "bind", "port");

    hostName = context.getString("bind");
    port = Integer.parseInt(context.getString("port"));
  }

  @Override
  public void start() {

    logger.info("Source starting");

    super.start();

    counterGroup.incrementAndGet("open.attempts");

    handlerService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("netcat-handler-%d").build());

    try {
      SocketAddress bindPoint = new InetSocketAddress(hostName, port);

      serverSocket = ServerSocketChannel.open();
      serverSocket.socket().setReuseAddress(true);
      serverSocket.socket().bind(bindPoint);

      logger.info("Created serverSocket:{}", serverSocket);
    } catch (IOException e) {
      counterGroup.incrementAndGet("open.errors");
      logger.error("Unable to bind to socket. Exception follows.", e);
      return;
    }

    AcceptHandler acceptRunnable = new AcceptHandler();

    acceptRunnable.counterGroup = counterGroup;
    acceptRunnable.handlerService = handlerService;
    acceptRunnable.shouldStop = acceptThreadShouldStop;
    acceptRunnable.source = this;
    acceptRunnable.serverSocket = serverSocket;

    acceptThread = new Thread(acceptRunnable);

    acceptThread.start();

    logger.debug("Source started");
  }

  @Override
  public void stop() {
    logger.info("Source stopping");

    super.stop();

    acceptThreadShouldStop.set(true);

    if (acceptThread != null) {
      logger.debug("Stopping accept handler thread");

      while (acceptThread.isAlive()) {
        try {
          logger.debug("Waiting for accept handler to finish");
          acceptThread.interrupt();
          acceptThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug("Interrupted while waiting for accept handler to finish");
          Thread.currentThread().interrupt();
        }
      }

      logger.debug("Stopped accept handler thread");
    }

    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Unable to close socket. Exception follows.", e);
        return;
      }
    }

    if (handlerService != null) {
      handlerService.shutdown();

      while (!handlerService.isTerminated()) {
        logger.debug("Waiting for handler service to stop");
        try {
          handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          logger
              .debug("Interrupted while waiting for netcat handler service to stop");
          handlerService.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }

      logger.debug("Handler service stopped");
    }

    logger.debug("Source stopped. Event metrics:{}", counterGroup);
  }

  public static class AcceptHandler implements Runnable {

    private ServerSocketChannel serverSocket;
    private CounterGroup counterGroup;
    private ExecutorService handlerService;
    private EventDrivenSource source;

    private AtomicBoolean shouldStop;

    @Override
    public void run() {
      logger.debug("Starting accept handler");

      while (!shouldStop.get()) {
        try {
          SocketChannel socketChannel = serverSocket.accept();

          NetcatSocketHandler request = new NetcatSocketHandler();

          request.socketChannel = socketChannel;
          request.counterGroup = counterGroup;
          request.source = source;

          handlerService.submit(request);

          counterGroup.incrementAndGet("accept.succeeded");
        } catch (ClosedByInterruptException e) {
          // Parent is canceling us.
        } catch (IOException e) {
          logger.error("Unable to accept connection. Exception follows.", e);
          counterGroup.incrementAndGet("accept.failed");
        }
      }

      logger.debug("Accept handler exiting");
    }
  }

  public static class NetcatSocketHandler implements Runnable {

    private Source source;

    private CounterGroup counterGroup;
    private SocketChannel socketChannel;

    @Override
    public void run() {
      Event event = null;

      try {
        Reader reader = Channels.newReader(socketChannel, "utf-8");
        Writer writer = Channels.newWriter(socketChannel, "utf-8");
        CharBuffer buffer = CharBuffer.allocate(512);
        StringBuilder builder = new StringBuilder();

        while (reader.read(buffer) != -1) {
          buffer.flip();

          logger.debug("read {} characters", buffer.remaining());

          counterGroup.addAndGet("characters.received",
              Long.valueOf(buffer.limit()));

          builder.append(buffer.array(), buffer.position(), buffer.length());
        }

        if (builder.charAt(builder.length() - 1) == '\n') {
          builder.deleteCharAt(builder.length() - 1);
        }

        event = EventBuilder.withBody(builder.toString().getBytes());
        Exception ex = null;

        try {
          source.getChannelProcessor().processEvent(event);
        } catch (ChannelException chEx) {
          ex = chEx;
        }

        if (ex == null) {
          writer.append("OK\n");
        } else {
          writer.append("FAILED: " + ex.getMessage() + "\n");
        }

        socketChannel.close();

        counterGroup.incrementAndGet("events.success");
      } catch (IOException e) {
        counterGroup.incrementAndGet("events.failed");
      }
    }
  }
}
