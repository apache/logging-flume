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
import java.nio.ByteBuffer;
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
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
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
 * <tr>
 * <td><tt>max-line-length</tt></td>
 * <td>The maximum # of chars a line can be per event (including newline).</td>
 * <td>Number of UTF-8 characters / int</td>
 * <td>512</td>
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
  private int maxLineLength;
  private boolean ackEveryEvent;

  private CounterGroup counterGroup;
  private ServerSocketChannel serverSocket;
  private AtomicBoolean acceptThreadShouldStop;
  private Thread acceptThread;
  private ExecutorService handlerService;

  public NetcatSource() {
    super();

    port = 0;
    counterGroup = new CounterGroup();
    acceptThreadShouldStop = new AtomicBoolean(false);
  }

  @Override
  public void configure(Context context) {
    String hostKey = NetcatSourceConfigurationConstants.CONFIG_HOSTNAME;
    String portKey = NetcatSourceConfigurationConstants.CONFIG_PORT;
    String ackEventKey = NetcatSourceConfigurationConstants.CONFIG_ACKEVENT;

    Configurables.ensureRequiredNonNull(context, hostKey, portKey);

    hostName = context.getString(hostKey);
    port = context.getInteger(portKey);
    ackEveryEvent = context.getBoolean(ackEventKey, true);
    maxLineLength = context.getInteger(
        NetcatSourceConfigurationConstants.CONFIG_MAX_LINE_LENGTH,
        NetcatSourceConfigurationConstants.DEFAULT_MAX_LINE_LENGTH);
  }

  @Override
  public void start() {

    logger.info("Source starting");

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
      throw new FlumeException(e);
    }

    AcceptHandler acceptRunnable = new AcceptHandler(maxLineLength);
    acceptThreadShouldStop.set(false);
    acceptRunnable.counterGroup = counterGroup;
    acceptRunnable.handlerService = handlerService;
    acceptRunnable.shouldStop = acceptThreadShouldStop;
    acceptRunnable.ackEveryEvent = ackEveryEvent;
    acceptRunnable.source = this;
    acceptRunnable.serverSocket = serverSocket;

    acceptThread = new Thread(acceptRunnable);

    acceptThread.start();

    logger.debug("Source started");
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Source stopping");

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

      logger.debug("Waiting for handler service to stop");

      // wait 500ms for threads to stop
      try {
        handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for netcat handler service to stop");
        Thread.currentThread().interrupt();
      }

      if (!handlerService.isShutdown()) {
        handlerService.shutdownNow();
      }

      logger.debug("Handler service stopped");
    }

    logger.debug("Source stopped. Event metrics:{}", counterGroup);
    super.stop();
  }

  private static class AcceptHandler implements Runnable {

    private ServerSocketChannel serverSocket;
    private CounterGroup counterGroup;
    private ExecutorService handlerService;
    private EventDrivenSource source;
    private AtomicBoolean shouldStop;
    private boolean ackEveryEvent;

    private final int maxLineLength;

    public AcceptHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting accept handler");

      while (!shouldStop.get()) {
        try {
          SocketChannel socketChannel = serverSocket.accept();

          NetcatSocketHandler request = new NetcatSocketHandler(maxLineLength);

          request.socketChannel = socketChannel;
          request.counterGroup = counterGroup;
          request.source = source;
          request.ackEveryEvent = ackEveryEvent;

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

  private static class NetcatSocketHandler implements Runnable {

    private Source source;
    private CounterGroup counterGroup;
    private SocketChannel socketChannel;
    private boolean ackEveryEvent;

    private final int maxLineLength;

    public NetcatSocketHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting connection handler");
      Event event = null;

      try {
        Reader reader = Channels.newReader(socketChannel, "utf-8");
        Writer writer = Channels.newWriter(socketChannel, "utf-8");
        CharBuffer buffer = CharBuffer.allocate(maxLineLength);
        buffer.flip(); // flip() so fill() sees buffer as initially empty

        while (true) {
          // this method blocks until new data is available in the socket
          int charsRead = fill(buffer, reader);
          logger.debug("Chars read = {}", charsRead);

          // attempt to process all the events in the buffer
          int eventsProcessed = processEvents(buffer, writer);
          logger.debug("Events processed = {}", eventsProcessed);

          if (charsRead == -1) {
            // if we received EOF before last event processing attempt, then we
            // have done everything we can
            break;
          } else if (charsRead == 0 && eventsProcessed == 0) {
            if (buffer.remaining() == buffer.capacity()) {
              // If we get here it means:
              // 1. Last time we called fill(), no new chars were buffered
              // 2. After that, we failed to process any events => no newlines
              // 3. The unread data in the buffer == the size of the buffer
              // Therefore, we are stuck because the client sent a line longer
              // than the size of the buffer. Response: Drop the connection.
              logger.warn("Client sent event exceeding the maximum length");
              counterGroup.incrementAndGet("events.failed");
              writer.write("FAILED: Event exceeds the maximum length (" +
                  buffer.capacity() + " chars, including newline)\n");
              writer.flush();
              break;
            }
          }
        }

        socketChannel.close();

        counterGroup.incrementAndGet("sessions.completed");
      } catch (IOException e) {
        counterGroup.incrementAndGet("sessions.broken");
      }

      logger.debug("Connection handler exiting");
    }

    /**
     * <p>Consume some number of events from the buffer into the system.</p>
     *
     * Invariants (pre- and post-conditions): <br/>
     *   buffer should have position @ beginning of unprocessed data. <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * @param buffer The buffer containing data to process
     * @param writer The channel back to the client
     * @return number of events successfully processed
     * @throws IOException
     */
    private int processEvents(CharBuffer buffer, Writer writer)
        throws IOException {

      int numProcessed = 0;

      boolean foundNewLine = true;
      while (foundNewLine) {
        foundNewLine = false;

        int limit = buffer.limit();
        for (int pos = buffer.position(); pos < limit; pos++) {
          if (buffer.get(pos) == '\n') {

            // parse event body bytes out of CharBuffer
            buffer.limit(pos); // temporary limit
            ByteBuffer bytes = Charsets.UTF_8.encode(buffer);
            buffer.limit(limit); // restore limit

            // build event object
            byte[] body = new byte[bytes.remaining()];
            bytes.get(body);
            Event event = EventBuilder.withBody(body);

            // process event
            ChannelException ex = null;
            try {
              source.getChannelProcessor().processEvent(event);
            } catch (ChannelException chEx) {
              ex = chEx;
            }

            if (ex == null) {
              counterGroup.incrementAndGet("events.processed");
              numProcessed++;
              if (true == ackEveryEvent) {
                writer.write("OK\n");
              }
            } else {
              counterGroup.incrementAndGet("events.failed");
              logger.warn("Error processing event. Exception follows.", ex);
              writer.write("FAILED: " + ex.getMessage() + "\n");
            }
            writer.flush();

            // advance position after data is consumed
            buffer.position(pos + 1); // skip newline
            foundNewLine = true;

            break;
          }
        }

      }

      return numProcessed;
    }

    /**
     * <p>Refill the buffer read from the socket.</p>
     *
     * Preconditions: <br/>
     *   buffer should have position @ beginning of unprocessed data. <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * Postconditions: <br/>
     *   buffer should have position @ beginning of buffer (pos=0). <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * Note: this method blocks on new data arriving.
     *
     * @param buffer The buffer to fill
     * @param reader The Reader to read the data from
     * @return number of characters read
     * @throws IOException
     */
    private int fill(CharBuffer buffer, Reader reader)
        throws IOException {

      // move existing data to the front of the buffer
      buffer.compact();

      // pull in as much data as we can from the socket
      int charsRead = reader.read(buffer);
      counterGroup.addAndGet("characters.received", Long.valueOf(charsRead));

      // flip so the data can be consumed
      buffer.flip();

      return charsRead;
    }

  }
}
