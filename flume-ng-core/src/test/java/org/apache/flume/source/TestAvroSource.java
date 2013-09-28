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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
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
import org.jboss.netty.handler.ssl.SslHandler;
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
  private InetAddress localhost;

  @Before
  public void setUp() throws UnknownHostException {
    localhost = InetAddress.getByName("127.0.0.1");
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
    NettyTransceiver nettyTransceiver;
    if (clientEnableCompression) {

      nettyTransceiver = new NettyTransceiver(new InetSocketAddress(
          selectedPort), new CompressionChannelFactory(compressionLevel));

      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
    } else {
      nettyTransceiver = new NettyTransceiver(new InetSocketAddress(selectedPort));

      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
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


    nettyTransceiver.close();
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

  @Test
  public void testSslRequest() throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 10 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");
        context.put("ssl", "true");
        context.put("keystore", "src/test/resources/server.p12");
        context.put("keystore-password", "password");
        context.put("keystore-type", "PKCS12");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
        Thread.sleep(100);
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
        selectedPort), new SSLChannelFactory()));

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();

    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));

    Status status = client.append(avroEvent);

    Assert.assertEquals(Status.OK, status);

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro ssl",
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

  /**
   * Factory of SSL-enabled client channels
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private static class SSLChannelFactory extends NioClientSocketChannelFactory {
    public SSLChannelFactory() {
      super(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      try {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[]{new PermissiveTrustManager()},
                        null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
        return super.newChannel(pipeline);
      } catch (Exception ex) {
        throw new RuntimeException("Cannot create SSL channel", ex);
      }
    }
  }

  /**
   * Bogus trust manager accepting any certificate
   */
  private static class PermissiveTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  @Test
  public void testValidIpFilterAllows()
      throws InterruptedException, IOException {
    doIpFilterTest(localhost, "allow:name:localhost,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:" + localhost.getHostAddress() +
        ",deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:127.0.0.2,allow:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:name:localhost,deny:ip:*", true, true);
    doIpFilterTest(localhost, "allow:ip:*", true, true);
  }

  @Test
  public void testValidIpFilterDenys()
      throws InterruptedException, IOException {
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "deny:name:localhost", false, false);
    doIpFilterTest(localhost, "deny:ip:" + localhost.getHostAddress() +
        ",allow:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "allow:ip:45.2.2.2,deny:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,allow:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:*", false, true);
  }

  @Test
  public void testInvalidIpFilter() throws InterruptedException, IOException {
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "allow:name:localhost", true, false);
    doIpFilterTest(localhost, "deny:ip:127.0.0.2,allow:ip:*,deny:ip:" +
        localhost.getHostAddress(), true, false);
    doIpFilterTest(localhost, "deny:ip:" +
        localhost.getHostAddress().substring(0, 3) + "*,allow:ip:*",
        false, false);
    try {
      doIpFilterTest(localhost, null, false, false);
      Assert.fail(
        "The null ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }

    try {
      doIpFilterTest(localhost, "", true, false);
      Assert.fail("The empty string ipFilterRules config should have thrown "
          + "an exception");
    } catch (FlumeException e) {
      //Do nothing
    }

    try {
      doIpFilterTest(localhost, "homer:ip:45.4.23.1", true, false);
      Assert.fail("Bad ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }
    try {
      doIpFilterTest(localhost, "allow:sleeps:45.4.23.1", true, false);
      Assert.fail("Bad ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }
  }

  public void doIpFilterTest(InetAddress dest, String ruleDefinition,
      boolean eventShouldBeAllowed, boolean testWithSSL)
      throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();
        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");
        context.put("ipFilter", "true");
        if (ruleDefinition != null) {
          context.put("ipFilterRules", ruleDefinition);
        }
        if (testWithSSL) {
          logger.info("Client testWithSSL" + testWithSSL);
          context.put("ssl", "true");
          context.put("keystore", "src/test/resources/server.p12");
          context.put("keystore-password", "password");
          context.put("keystore-type", "PKCS12");
        }

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
        Thread.sleep(100);
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client;
    NettyTransceiver nettyTransceiver = null;
    try {
      if (testWithSSL) {
        nettyTransceiver = new NettyTransceiver(
          new InetSocketAddress (dest, selectedPort),
          new SSLChannelFactory());
        client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
      } else {
        nettyTransceiver = new NettyTransceiver(
          new InetSocketAddress (dest, selectedPort));
        client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
      }

      AvroFlumeEvent avroEvent = new AvroFlumeEvent();
      avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
      avroEvent.setBody(ByteBuffer.wrap("Hello avro ipFilter".getBytes()));

      logger.info("Client about to append");
      Status status = client.append(avroEvent);
      logger.info("Client appended");
      Assert.assertEquals(Status.OK, status);
    } catch (IOException e) {
      Assert.assertTrue("Should have been allowed: " + ruleDefinition,
        !eventShouldBeAllowed);
      return;
    } finally {
      if (nettyTransceiver != null) {
        nettyTransceiver.close();
      }
      source.stop();
    }
    Assert.assertTrue("Should have been denied: " + ruleDefinition,
        eventShouldBeAllowed);

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro ipFilter",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();
    logger.debug("Round trip event:{}", event);

    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }
}
