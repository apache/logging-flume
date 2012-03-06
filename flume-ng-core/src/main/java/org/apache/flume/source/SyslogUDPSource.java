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
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.SyslogUtils;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
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

public class SyslogUDPSource extends AbstractSource
      implements EventDrivenSource, Configurable {
  final public static int SYSLOG_UDP_PORT = 514;

  private int port = SYSLOG_UDP_PORT; // default udp syslog port
  private int maxsize = 1 << 16; // 64k is max allowable in RFC 5426
  private String host = null;
  private Channel nettyChannel;

  private static final Logger logger = LoggerFactory
      .getLogger(SyslogUDPSource.class);


  public class syslogHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      try {
        Event e = SyslogUtils.extractEvent((ChannelBuffer)mEvent.getMessage());
        if (e == null) {
          return;
        }
        getChannelProcessor().processEvent(e);
      } catch (ChannelException ex) {
        logger.error("Error writting to channel", ex);
        return;
      } catch (IOException eI) {
        logger.error("Error reading from network", eI);
      }
    }
  }

  @Override
  public void start() {
    // setup Netty server
    ConnectionlessBootstrap serverBootstrap = new ConnectionlessBootstrap
        (new OioDatagramChannelFactory(Executors.newCachedThreadPool()));
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() {
       return Channels.pipeline(new syslogHandler());
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
    port = Integer.parseInt(context.getString("port"));
    host = context.getString("host");
  }

}
