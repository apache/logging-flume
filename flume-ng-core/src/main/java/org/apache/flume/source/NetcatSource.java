package org.apache.flume.source;

import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.durability.WALManager;
import org.apache.flume.durability.WALWriter;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class NetcatSource extends AbstractEventSource implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(NetcatSource.class);

  private int port;
  private String nodeName;
  private ServerSocketChannel serverSocket;
  private CounterGroup counterGroup;

  private WALManager walManager;
  private WALWriter walWriter;

  public NetcatSource() {
    port = 0;
    counterGroup = new CounterGroup();
  }

  @Override
  public void configure(Context context) {
    String nodeName = context.get("logicalNode.name", String.class);
    String port = context.get("source.port", String.class);

    Preconditions.checkArgument(nodeName != null, "Node name may not be null");
    Preconditions.checkArgument(port != null, "Source port may not be null");

    this.nodeName = nodeName;
    this.port = Integer.parseInt(port);
  }

  @Override
  public void open(Context context) throws LifecycleException {
    counterGroup.incrementAndGet("open.attempts");

    try {
      SocketAddress bindPoint = new InetSocketAddress(port);

      serverSocket = ServerSocketChannel.open();
      serverSocket.socket().setReuseAddress(true);
      serverSocket.socket().bind(bindPoint);

      logger.info("Created serverSocket:{}", serverSocket);
    } catch (IOException e) {
      counterGroup.incrementAndGet("open.errors");
      logger.error("Unable to bind to socket. Exception follows.", e);
    }

    if (walManager != null) {
      logger.debug("Event durability features enabled. Using WALManager:{}",
          walManager);
      try {
        walWriter = walManager.getWAL(nodeName).getWriter();
      } catch (IOException e) {
        throw new LifecycleException(
            "Unable to get WAL writer. Exception follows.", e);
      }
    }
  }

  @Override
  public Event next(Context context) throws InterruptedException,
      EventDeliveryException {

    Event event = null;

    counterGroup.incrementAndGet("next.calls");

    try {
      SocketChannel channel = serverSocket.accept();

      logger.debug("Received a connection:{}", channel);

      Reader reader = Channels.newReader(channel, "utf-8");
      CharBuffer buffer = CharBuffer.allocate(512);
      StringBuilder builder = new StringBuilder();

      while (reader.read(buffer) != -1) {
        buffer.flip();
        logger.debug("read {} characters", buffer.remaining());
        builder.append(buffer.array(), buffer.position(), buffer.length());
      }

      if (builder.charAt(builder.length() - 1) == '\n') {
        builder.deleteCharAt(builder.length() - 1);
      }

      logger.debug("end of message");

      event = EventBuilder.withBody(builder.toString().getBytes());

      if (walWriter != null) {
        walWriter.write(event);
        walWriter.flush();
      }

      channel.close();

      counterGroup.incrementAndGet("events.success");
    } catch (IOException e) {
      counterGroup.incrementAndGet("events.failed");

      throw new EventDeliveryException("Unable to process event due to "
          + e.getMessage(), e);
    }

    return event;
  }

  @Override
  public void close(Context context) throws LifecycleException {
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Unable to close socket. Exception follows.", e);
      }
    }

    if (walWriter != null) {
      try {
        walWriter.flush();
        walWriter.close();
      } catch (IOException e) {
        throw new LifecycleException(
            "Unable to flush WAL on close - POTENTIAL DATA LOSS! Exception follows.",
            e);
      }
    }
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public WALManager getWALManager() {
    return walManager;
  }

  public void setWALManager(WALManager walManager) {
    this.walManager = walManager;
  }

}
