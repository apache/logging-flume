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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MultiportSyslogTCPSource extends AbstractSource implements
        EventDrivenSource, Configurable {

  public static final Logger logger = LoggerFactory.getLogger(
          MultiportSyslogTCPSource.class);

  private final ConcurrentMap<Integer, ThreadSafeDecoder> portCharsets;

  private List<Integer> ports = Lists.newArrayList();
  private String host;
  private NioSocketAcceptor acceptor;
  private Integer numProcessors;
  private int maxEventSize;
  private int batchSize;
  private int readBufferSize;
  private String portHeader;
  private SourceCounter sourceCounter = null;
  private Charset defaultCharset;
  private ThreadSafeDecoder defaultDecoder;
  private boolean keepFields;

  public MultiportSyslogTCPSource() {
    portCharsets = new ConcurrentHashMap<Integer, ThreadSafeDecoder>();
  }

  @Override
  public void configure(Context context) {
    String portsStr = context.getString(
            SyslogSourceConfigurationConstants.CONFIG_PORTS);

    Preconditions.checkNotNull(portsStr, "Must define config "
            + "parameter for MultiportSyslogTCPSource: ports");

    for (String portStr : portsStr.split("\\s+")) {
      Integer port = Integer.parseInt(portStr);
      ports.add(port);
    }

    host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);

    numProcessors = context.getInteger(
            SyslogSourceConfigurationConstants.CONFIG_NUMPROCESSORS);

    maxEventSize = context.getInteger(
        SyslogSourceConfigurationConstants.CONFIG_EVENTSIZE,
        SyslogUtils.DEFAULT_SIZE);

    String defaultCharsetStr = context.getString(
        SyslogSourceConfigurationConstants.CONFIG_CHARSET,
        SyslogSourceConfigurationConstants.DEFAULT_CHARSET);
    try {
      defaultCharset = Charset.forName(defaultCharsetStr);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Unable to parse charset "
          + "string (" + defaultCharsetStr + ") from port configuration.", ex);

    }

    defaultDecoder = new ThreadSafeDecoder(defaultCharset);

    // clear any previous charset configuration and reconfigure it
    portCharsets.clear();
    {
      ImmutableMap<String, String> portCharsetCfg = context.getSubProperties(
        SyslogSourceConfigurationConstants.CONFIG_PORT_CHARSET_PREFIX);
      for (Map.Entry<String, String> entry : portCharsetCfg.entrySet()) {
        String portStr = entry.getKey();
        String charsetStr = entry.getValue();
        Integer port = Integer.parseInt(portStr);
        Preconditions.checkNotNull(port, "Invalid port number in config");
        try {
          Charset charset = Charset.forName(charsetStr);
          portCharsets.put(port, new ThreadSafeDecoder(charset));
        } catch (Exception ex) {
          throw new IllegalArgumentException("Unable to parse charset " +
              "string (" + charsetStr + ") from port configuration.", ex);
        }
      }
    }

    batchSize = context.getInteger(
        SyslogSourceConfigurationConstants.CONFIG_BATCHSIZE,
        SyslogSourceConfigurationConstants.DEFAULT_BATCHSIZE);

    portHeader = context.getString(
            SyslogSourceConfigurationConstants.CONFIG_PORT_HEADER);

    readBufferSize = context.getInteger(
        SyslogSourceConfigurationConstants.CONFIG_READBUF_SIZE,
        SyslogSourceConfigurationConstants.DEFAULT_READBUF_SIZE);

    keepFields = context.getBoolean(
        SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
        SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    // allow user to specify number of processors to use for thread pool
    if (numProcessors != null) {
      acceptor = new NioSocketAcceptor(numProcessors);
    } else {
      acceptor = new NioSocketAcceptor();
    }
    acceptor.setReuseAddress(true);
    acceptor.getSessionConfig().setReadBufferSize(readBufferSize);
    acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

    acceptor.setHandler(new MultiportSyslogHandler(maxEventSize, batchSize,
        getChannelProcessor(), sourceCounter, portHeader, defaultDecoder,
        portCharsets, keepFields));

    for (int port : ports) {
      InetSocketAddress addr;
      if (host != null) {
        addr = new InetSocketAddress(host, port);
      } else {
        addr = new InetSocketAddress(port);
      }
      try {
        //Not using the one that takes an array because we won't want one bind
        //error affecting the next.
        acceptor.bind(addr);
      } catch (IOException ex) {
        logger.error("Could not bind to address: " + String.valueOf(addr), ex);
      }
    }

    sourceCounter.start();
    super.start();

    logger.info("{} started.", this);
  }

  @Override
  public void stop() {
    logger.info("Stopping {}...", this);

    acceptor.unbind();
    acceptor.dispose();

    sourceCounter.stop();
    super.stop();

    logger.info("{} stopped. Metrics: {}", this, sourceCounter);
  }

  @Override
  public String toString() {
    return "Multiport Syslog TCP source " + getName();
  }

  static class MultiportSyslogHandler extends IoHandlerAdapter {

    private static final String SAVED_BUF = "savedBuffer";
    private final ChannelProcessor channelProcessor;
    private final int maxEventSize;
    private final int batchSize;
    private final SourceCounter sourceCounter;
    private final String portHeader;
    private final SyslogParser syslogParser;
    private final LineSplitter lineSplitter;
    private final ThreadSafeDecoder defaultDecoder;
    private final ConcurrentMap<Integer, ThreadSafeDecoder> portCharsets;
    private final boolean keepFields;

    public MultiportSyslogHandler(int maxEventSize, int batchSize,
        ChannelProcessor cp, SourceCounter ctr, String portHeader,
        ThreadSafeDecoder defaultDecoder,
        ConcurrentMap<Integer, ThreadSafeDecoder> portCharsets, boolean keepFields) {
      channelProcessor = cp;
      sourceCounter = ctr;
      this.maxEventSize = maxEventSize;
      this.batchSize = batchSize;
      this.portHeader = portHeader;
      this.defaultDecoder = defaultDecoder;
      this.portCharsets = portCharsets;
      this.keepFields = keepFields;
      syslogParser = new SyslogParser();
      lineSplitter = new LineSplitter(maxEventSize);
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause)
        throws Exception {
      logger.error("Error in syslog message handler", cause);
      if (cause instanceof Error) {
        Throwables.propagate(cause);
      }
    }

    @Override
    public void sessionCreated(IoSession session) {
      logger.info("Session created: {}", session);

      // Allocate saved buffer when session is created.
      // This allows us to parse an incomplete message and use it on
      // the next request.
      session.setAttribute(SAVED_BUF, IoBuffer.allocate(maxEventSize, false));
    }

    @Override
    public void sessionOpened(IoSession session) {
      // debug level so it isn't too spammy together w/ sessionCreated()
      logger.debug("Session opened: {}", session);
    }

    @Override
    public void sessionClosed(IoSession session) {
      logger.info("Session closed: {}", session);
    }

    @Override
    public void messageReceived(IoSession session, Object message) {

      IoBuffer buf = (IoBuffer) message;
      IoBuffer savedBuf = (IoBuffer) session.getAttribute(SAVED_BUF);

      ParsedBuffer parsedLine = new ParsedBuffer();
      List<Event> events = Lists.newArrayList();

      // the character set can be specified per-port
      CharsetDecoder decoder = defaultDecoder.get();
      int port =
          ((InetSocketAddress) session.getLocalAddress()).getPort();
      if (portCharsets.containsKey(port)) {
        decoder = portCharsets.get(port).get();
      }

      // while the buffer is not empty
      while (buf.hasRemaining()) {
        events.clear();

        // take number of events no greater than batchSize
        for (int num = 0; num < batchSize && buf.hasRemaining(); num++) {

          if (lineSplitter.parseLine(buf, savedBuf, parsedLine)) {
            Event event = parseEvent(parsedLine, decoder);
            if (portHeader != null) {
              event.getHeaders().put(portHeader, String.valueOf(port));
            }
            events.add(event);
          } else {
            logger.trace("Parsed null event");
          }

        }

        // don't try to write anything if we didn't get any events somehow
        if (events.isEmpty()) {
          logger.trace("Empty set!");
          return;
        }

        int numEvents = events.size();
        sourceCounter.addToEventReceivedCount(numEvents);

        // write the events to the downstream channel
        try {
          channelProcessor.processEventBatch(events);
          sourceCounter.addToEventAcceptedCount(numEvents);
        } catch (Throwable t) {
          logger.error("Error writing to channel, event dropped", t);
          if (t instanceof Error) {
            Throwables.propagate(t);
          }
        }
      }

    }

    /**
     * Decodes a syslog-formatted ParsedLine into a Flume Event.
     * @param parsedBuf Buffer containing characters to be parsed
     * @param decoder Character set is configurable on a per-port basis.
     * @return
     */
    Event parseEvent(ParsedBuffer parsedBuf, CharsetDecoder decoder) {
      String msg = null;
      try {
        msg = parsedBuf.buffer.getString(decoder);
      } catch (Throwable t) {
        logger.info("Error decoding line with charset (" + decoder.charset() +
            "). Exception follows.", t);

        if (t instanceof Error) {
          Throwables.propagate(t);
        }

        // fall back to byte array
        byte[] bytes = new byte[parsedBuf.buffer.remaining()];
        parsedBuf.buffer.get(bytes);

        Event event = EventBuilder.withBody(bytes);
        event.getHeaders().put(SyslogUtils.EVENT_STATUS,
            SyslogUtils.SyslogStatus.INVALID.getSyslogStatus());

        return event;
      }

      logger.trace("Seen raw event: {}", msg);

      Event event;
      try {
        event = syslogParser.parseMessage(msg, decoder.charset(), keepFields);
        if (parsedBuf.incomplete) {
          event.getHeaders().put(SyslogUtils.EVENT_STATUS,
              SyslogUtils.SyslogStatus.INCOMPLETE.getSyslogStatus());
        }
      } catch (IllegalArgumentException ex) {
        event = EventBuilder.withBody(msg, decoder.charset());
        event.getHeaders().put(SyslogUtils.EVENT_STATUS,
            SyslogUtils.SyslogStatus.INVALID.getSyslogStatus());
        logger.debug("Error parsing syslog event", ex);
      }

      return event;
    }
  }

  /**
   * This class is designed to parse lines up to a maximum length. If the line
   * exceeds the given length, it is cut off at that mark and an overflow flag
   * is set for the line. If less than the specified length is parsed, and a
   * newline is not found, then the parsed data is saved in a buffer provided
   * for that purpose so that it can be used in the next round of parsing.
   */
  static class LineSplitter {

    private final static byte NEWLINE = '\n';
    private final int maxLineLength;

    public LineSplitter(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    /**
     * Parse a line from the IoBuffer {@code buf} and store it into
     * {@code parsedBuf} except for the trailing newline character. If a line
     * is successfully parsed, returns {@code true}.
     * <p/>If no newline is found, and
     * the number of bytes traversed is less than {@code maxLineLength}, then
     * the data read from {@code buf} is stored in {@code savedBuf} and this
     * method returns {@code false}.
     * <p/>If the number of characters traversed
     * equals {@code maxLineLength}, but a newline was not found, then the
     * {@code parsedBuf} variable will be populated, the {@code overflow} flag
     * will be set in the {@code ParsedBuffer} object, and this function will
     * return {@code true}.
     */
    public boolean parseLine(IoBuffer buf, IoBuffer savedBuf,
        ParsedBuffer parsedBuf) {

      // clear out passed-in ParsedBuffer object
      parsedBuf.buffer = null;
      parsedBuf.incomplete = false;

      byte curByte;

      buf.mark();
      int msgPos = savedBuf.position(); // carry on from previous buffer
      boolean seenNewline = false;
      while (!seenNewline && buf.hasRemaining() && msgPos < maxLineLength) {
        curByte = buf.get();

        // we are looking for newline delimiters between events
        if (curByte == NEWLINE) {
          seenNewline = true;
        }

        msgPos++;
      }

      // hit a newline?
      if (seenNewline) {

        int end = buf.position();
        buf.reset();
        int start = buf.position();

        if (savedBuf.position() > 0) {
          // complete the saved buffer
          byte[] tmp = new byte[end - start];
          buf.get(tmp);
          savedBuf.put(tmp);
          int len = savedBuf.position() - 1;
          savedBuf.flip();

          parsedBuf.buffer = savedBuf.getSlice(len);

          savedBuf.clear();
        } else {
          parsedBuf.buffer = buf.getSlice(end - start - 1);

          buf.get();  // throw away newline
        }

        return true;

      // we either emptied our buffer or hit max msg size
      } else {

        // exceeded max message size
        if (msgPos == maxLineLength) {

          int end = buf.position();
          buf.reset();
          int start = buf.position();

          if (savedBuf.position() > 0) {
            // complete the saved buffer
            byte[] tmp = new byte[end - start];
            buf.get(tmp);
            savedBuf.put(tmp);
            savedBuf.flip();
            parsedBuf.buffer = savedBuf.getSlice(msgPos);
            savedBuf.clear();
          } else {
            // no newline found
            parsedBuf.buffer = buf.getSlice(msgPos);
          }

          logger.warn("Event size larger than specified event size: {}. "
              + "Consider increasing the max event size.", maxLineLength);

          parsedBuf.incomplete = true;

          return true;

        // message fragmentation; save in buffer for later
        } else if (!buf.hasRemaining()) {

          int end = buf.position();
          buf.reset();
          int start = buf.position();
          byte[] tmp = new byte[end - start];
          buf.get(tmp);
          savedBuf.put(tmp);

          return false;

        // this should never happen
        } else {

          throw new IllegalStateException("unexpected buffer state: " +
              "msgPos=" + msgPos + ", buf.hasRemaining=" + buf.hasRemaining() +
              ", savedBuf.hasRemaining=" + savedBuf.hasRemaining() +
              ", seenNewline=" + seenNewline + ", maxLen=" + maxLineLength);

        }

      }
    }

  }

  /**
   * Private struct to represent a simple text line parsed from a message.
   */
  static class ParsedBuffer {

    /**
     * The parsed line of text, without the newline character.
     */
    public IoBuffer buffer = null;
    /**
     * The incomplete flag is set if the source line length exceeds the maximum
     * allowed line length. In that case, the returned line will have length
     * equal to the maximum line length.
     */
    public boolean incomplete = false;
  }

  /**
   * Package private only for unit testing
   */
  static class ThreadSafeDecoder extends ThreadLocal<CharsetDecoder> {
    private final Charset charset;

    public ThreadSafeDecoder(Charset charset) {
      this.charset = charset;
    }

    @Override
    protected CharsetDecoder initialValue() {
      return charset.newDecoder();
    }
  }

}
