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

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetcatUdpSource extends AbstractSource
      implements EventDrivenSource, Configurable {

  private int port;
  private int maxsize = 1 << 16; // 64k
  private String host = null;
  private Channel nettyChannel;
  private String remoteHostHeader = "REMOTE_ADDRESS";

  private static final Logger logger = LoggerFactory
      .getLogger(NetcatUdpSource.class);

  private CounterGroup counterGroup = new CounterGroup();

  // Default Min size
  private static final int DEFAULT_MIN_SIZE = 2048;
  private static final int DEFAULT_INITIAL_SIZE = DEFAULT_MIN_SIZE;
  private static final String REMOTE_ADDRESS_HEADER = "remoteAddress";
  private static final String CONFIG_PORT = "port";
  private static final String CONFIG_HOST = "bind";

  public class NetcatHandler extends SimpleChannelHandler {


    // extract line for building Flume event
    private Event extractEvent(ChannelBuffer in, SocketAddress remoteAddress) {

      Map<String, String> headers = new HashMap<String,String>();

      headers.put(remoteHostHeader, remoteAddress.toString());

      byte b = 0;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Event e = null;
      boolean doneReading = false;

      try {
        while (!doneReading && in.readable()) {
          b = in.readByte();
          // Entries are separated by '\n'
          if (b == '\n') {
            doneReading = true;
          } else {
            baos.write(b);
          }
        }

        e = EventBuilder.withBody(baos.toByteArray(), headers);
      } finally {
        // no-op
      }

      return e;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      try {
        Event e = extractEvent((ChannelBuffer)mEvent.getMessage(), mEvent.getRemoteAddress());
        if (e == null) {
          return;
        }
        getChannelProcessor().processEvent(e);
        counterGroup.incrementAndGet("events.success");
      } catch (ChannelException ex) {
        counterGroup.incrementAndGet("events.dropped");
        logger.error("Error writing to channel", ex);
      } catch (RuntimeException ex) {
        counterGroup.incrementAndGet("events.dropped");
        logger.error("Error retrieving event from udp stream, event dropped", ex);
      }
    }
  }

  @Override
  public void start() {
    // setup Netty server
    ConnectionlessBootstrap serverBootstrap = new ConnectionlessBootstrap(
        new OioDatagramChannelFactory(Executors.newCachedThreadPool()));
    final NetcatHandler handler = new NetcatHandler();
    serverBootstrap.setOption("receiveBufferSizePredictorFactory",
        new AdaptiveReceiveBufferSizePredictorFactory(DEFAULT_MIN_SIZE,
        DEFAULT_INITIAL_SIZE, maxsize));
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(handler);
      }
    });

    if (host == null) {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
    } else {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("Netcat UDP Source stopping...");
    logger.info("Metrics:{}", counterGroup);
    if (nettyChannel != null) {
      nettyChannel.close();
      try {
        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("netty server stop interrupted", e);
      } finally {
        nettyChannel = null;
      }
    }

    super.stop();
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(
        context, CONFIG_PORT);
    port = context.getInteger(CONFIG_PORT);
    host = context.getString(CONFIG_HOST);
    remoteHostHeader = context.getString(REMOTE_ADDRESS_HEADER);
  }

  @VisibleForTesting
  public int getSourcePort() {
    SocketAddress localAddress = nettyChannel.getLocalAddress();
    if (localAddress instanceof InetSocketAddress) {
      InetSocketAddress addr = (InetSocketAddress) localAddress;
      return addr.getPort();
    }
    return 0;
  }
}
