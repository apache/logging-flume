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
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
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

public class SyslogUDPSource extends AbstractSource
                             implements EventDrivenSource, Configurable {

  private int port;
  private int maxsize = 1 << 16; // 64k is max allowable in RFC 5426
  private String host = null;
  private Channel nettyChannel;
  private Map<String, String> formaterProp;
  private Set<String> keepFields;

  private static final Logger logger = LoggerFactory.getLogger(SyslogUDPSource.class);

  private SourceCounter sourceCounter;

  // Default Min size
  public static final int DEFAULT_MIN_SIZE = 2048;
  public static final int DEFAULT_INITIAL_SIZE = DEFAULT_MIN_SIZE;

  public class syslogHandler extends SimpleChannelHandler {
    private SyslogUtils syslogUtils = new SyslogUtils(DEFAULT_INITIAL_SIZE, null, true);

    public void setFormater(Map<String, String> prop) {
      syslogUtils.addFormats(prop);
    }

    public void setKeepFields(Set<String> keepFields) {
      syslogUtils.setKeepFields(keepFields);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      try {
        syslogUtils.setEventSize(maxsize);
        Event e = syslogUtils.extractEvent((ChannelBuffer)mEvent.getMessage());
        if (e == null) {
          return;
        }
        sourceCounter.incrementEventReceivedCount();

        getChannelProcessor().processEvent(e);
        sourceCounter.incrementEventAcceptedCount();
      } catch (ChannelException ex) {
        logger.error("Error writting to channel", ex);
        return;
      } catch (RuntimeException ex) {
        logger.error("Error parsing event from syslog stream, event dropped", ex);
        return;
      }
    }
  }

  @Override
  public void start() {
    // setup Netty server
    ConnectionlessBootstrap serverBootstrap = new ConnectionlessBootstrap(
        new OioDatagramChannelFactory(Executors.newCachedThreadPool()));
    final syslogHandler handler = new syslogHandler();
    handler.setFormater(formaterProp);
    handler.setKeepFields(keepFields);
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

    sourceCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Syslog UDP Source stopping...");
    logger.info("Metrics: {}", sourceCounter);
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

    sourceCounter.stop();
    super.stop();
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(
        context, SyslogSourceConfigurationConstants.CONFIG_PORT);
    port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
    host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
    formaterProp = context.getSubProperties(
        SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
    keepFields = SyslogUtils.chooseFieldsToKeep(
        context.getString(
            SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
            SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS));

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @VisibleForTesting
  InetSocketAddress getBoundAddress() {
    SocketAddress localAddress = nettyChannel.getLocalAddress();
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
