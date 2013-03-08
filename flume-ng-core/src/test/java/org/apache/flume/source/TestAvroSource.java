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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSource {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSource.class);

  private int selectedPort;
  private AvroSource source;
  private Channel channel;

  @Before
  public void setUp() {
    source = new AvroSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  @Test
  public void testRequestWithNoCompression() throws InterruptedException, IOException {

    doRequest(false, false, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel0() throws InterruptedException, IOException {

    doRequest(true, true, 0);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel1() throws InterruptedException, IOException {

    doRequest(true, true, 1);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel6() throws InterruptedException, IOException {

    doRequest(true, true, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel9() throws InterruptedException, IOException {

    doRequest(true, true, 9);
  }

  @Test(expected=org.apache.avro.AvroRemoteException.class)
  public void testRequestWithCompressionOnServerOnly() throws InterruptedException, IOException {
    //This will fail because both client and server need compression on
    doRequest(true, false, 6);
  }

  @Test(expected=org.apache.avro.AvroRemoteException.class)
  public void testRequestWithCompressionOnClientOnly() throws InterruptedException, IOException {
    //This will fail because both client and server need compression on
    doRequest(false, true, 6);
  }

  private void doRequest(boolean serverEnableCompression, boolean clientEnableCompression, int compressionLevel) throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();
        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");
        context.put("threads", "50");
        if (serverEnableCompression) {
          context.put("compression-type", "deflate");
        } else {
          context.put("compression-type", "none");
        }

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client;
    if (clientEnableCompression) {
      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
              selectedPort), new CompressionChannelFactory(6)));
    } else {
      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
              selectedPort)));
    }

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();

    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro".getBytes()));

    Status status = client.append(avroEvent);

    Assert.assertEquals(Status.OK, status);

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();

    logger.debug("Round trip event:{}", event);

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }


  private static class CompressionChannelFactory extends
      NioClientSocketChannelFactory {
    private int compressionLevel;

    public CompressionChannelFactory( int compressionLevel) {
      super();
      this.compressionLevel = compressionLevel;
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      try {

        ZlibEncoder encoder = new ZlibEncoder(compressionLevel);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
        return super.newChannel(pipeline);
      } catch (Exception ex) {
        throw new RuntimeException("Cannot create Compression channel", ex);
      }
    }
  }

}
