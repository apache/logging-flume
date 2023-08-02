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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLEngine;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;

/**
 * @deprecated use {@link MultiportSyslogTCPSource} instead.
 */
@Deprecated
public class SyslogTcpSource extends SslContextAwareAbstractSource
    implements EventDrivenSource, Configurable {
  private static final Logger logger = LoggerFactory.getLogger(SyslogTcpSource.class);

  private int port;
  private String host = null;
  private Integer eventSize;
  private Map<String, String> formaterProp;
  private SourceCounter sourceCounter;
  private Set<String> keepFields;
  private String clientIPHeader;
  private String clientHostnameHeader;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;

  public class SyslogTcpHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final SyslogUtils syslogUtils = new SyslogUtils();
    private final String clientIPHeader;
    private final String clientHostnameHeader;

    SyslogTcpHandler(int eventSize, Set<String> keepFields, Map<String, String> formatterProp,
            String clientIPHeader, String clientHostnameHeader) {
      syslogUtils.setEventSize(eventSize);
      syslogUtils.addFormats(formatterProp);
      syslogUtils.setKeepFields(keepFields);
      this.clientIPHeader = clientIPHeader;
      this.clientHostnameHeader = clientHostnameHeader;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buff) throws Exception {
      while (buff.isReadable()) {
        Event e = syslogUtils.extractEvent(buff);
        if (e == null) {
          logger.debug("Parsed partial event, event will be generated when " +
              "rest of the event is received.");
          continue;
        }

        if (clientIPHeader != null) {
          e.getHeaders().put(clientIPHeader,
              SyslogUtils.getIP(ctx.channel().remoteAddress()));
        }

        if (clientHostnameHeader != null) {
          e.getHeaders().put(clientHostnameHeader,
              SyslogUtils.getHostname(ctx.channel().remoteAddress()));
        }

        sourceCounter.incrementEventReceivedCount();

        try {
          getChannelProcessor().processEvent(e);
          sourceCounter.incrementEventAcceptedCount();
        } catch (ChannelException ex) {
          logger.error("Error writting to channel, event dropped", ex);
          sourceCounter.incrementChannelWriteFail();
        } catch (RuntimeException ex) {
          logger.error("Error parsing event from syslog stream, event dropped", ex);
          sourceCounter.incrementEventReadFail();
          return;
        }
      }

    }
  }

  @Override
  public void start() {
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();

    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
              .channel(NioServerSocketChannel.class)
              .option(ChannelOption.SO_BACKLOG, 100)
              .handler(new LoggingHandler(LogLevel.TRACE))
              .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline p = ch.pipeline();
                  p.addLast(new SyslogTcpHandler(eventSize, keepFields, formaterProp, clientIPHeader,
                      clientHostnameHeader));
                  Optional<SSLEngine> engine = getSslEngine(false);
                  engine.ifPresent(sslEngine -> {
                    ch.pipeline().addFirst("ssl", new SslHandler(sslEngine));
                  });
                }
              });
      // Start the server.
      logger.info("Syslog TCP Source starting...");
      serverChannel = b.bind(host, port).sync().channel();
    } catch (Exception ex) {
      logger.error("Unable to start Syslog TCP Source", ex);
    }

    sourceCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Syslog TCP Source stopping...");
    logger.info("Metrics: {}", sourceCounter);

    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    try {
      serverChannel.closeFuture().sync();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    sourceCounter.stop();
    super.stop();
  }

  @Override
  public void configure(Context context) {
    configureSsl(context);
    Configurables.ensureRequiredNonNull(context,
        SyslogSourceConfigurationConstants.CONFIG_PORT);
    port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
    host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
    eventSize = context.getInteger("eventSize", SyslogUtils.DEFAULT_SIZE);
    formaterProp = context.getSubProperties(SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
    keepFields = SyslogUtils.chooseFieldsToKeep(
        context.getString(
            SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
            SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS));
    clientIPHeader = context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_IP_HEADER);
    clientHostnameHeader = context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_HOSTNAME_HEADER);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @VisibleForTesting
  InetSocketAddress getBoundAddress() {
    SocketAddress localAddress = serverChannel.localAddress();
    if (!(localAddress instanceof InetSocketAddress)) {
      throw new IllegalArgumentException("Not bound to an internet address");
    }
    return (InetSocketAddress) localAddress;
  }

  @VisibleForTesting
  SourceCounter getSourceCounter() {
    return sourceCounter;
  }
}
