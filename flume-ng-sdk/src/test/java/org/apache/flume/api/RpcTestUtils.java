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
package org.apache.flume.api;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.netty.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.JZlibDecoder;
import io.netty.handler.codec.compression.JZlibEncoder;
import io.netty.handler.codec.compression.ZlibEncoder;
import junit.framework.Assert;

/**
 * Helpers for Netty Avro RPC testing
 */
public class RpcTestUtils {

  private static final Logger logger = LoggerFactory.getLogger(RpcTestUtils.class);

  private static final String localhost = "localhost";

  /**
   * Helper method for testing simple (single) appends on handlers
   * @param handler
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  public static void handlerSimpleAppendTest(AvroSourceProtocol handler)
      throws FlumeException, EventDeliveryException {
    handlerSimpleAppendTest(handler, false, false, 0);
  }

  /**
   * Helper method for testing simple (single) with compression level 6 appends on handlers
   * @param handler
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  public static void handlerSimpleAppendTest(AvroSourceProtocol handler,
                                             boolean enableServerCompression,
                                             boolean enableClientCompression, int compressionLevel)
      throws FlumeException, EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = startServer(handler, 0, enableServerCompression);
    try {
      Properties starterProp = new Properties();
      if (enableClientCompression) {
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "deflate");
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL,
                                "" + compressionLevel);
      } else {
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "none");
      }
      client = getStockLocalClient(server.getPort(), starterProp);
      boolean isActive = client.isActive();
      Assert.assertTrue("Client should be active", isActive);
      client.append(EventBuilder.withBody("wheee!!!", Charset.forName("UTF8")));
    } finally {
      stopServer(server);
      if (client != null) client.close();
    }
  }

  public static void handlerBatchAppendTest(AvroSourceProtocol handler)
      throws FlumeException, EventDeliveryException {
    handlerBatchAppendTest(handler, false, false, 0);
  }

  /**
   * Helper method for testing batch appends on handlers
   * @param handler
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  public static void handlerBatchAppendTest(AvroSourceProtocol handler,
                                            boolean enableServerCompression,
                                            boolean enableClientCompression, int compressionLevel)
      throws FlumeException, EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = startServer(handler, 0 , enableServerCompression);
    try {

      Properties starterProp = new Properties();
      if (enableClientCompression) {
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "deflate");
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL,
                                "" + compressionLevel);
      } else {
        starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE, "none");
      }

      client = getStockLocalClient(server.getPort(), starterProp);
      boolean isActive = client.isActive();
      Assert.assertTrue("Client should be active", isActive);

      int batchSize = client.getBatchSize();
      List<Event> events = new ArrayList<Event>();
      for (int i = 0; i < batchSize; i++) {
        events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
      }
      client.appendBatch(events);

    } finally {
      stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * Helper method for constructing a Netty RPC client that talks to localhost.
   */
  public static NettyAvroRpcClient getStockLocalClient(int port) {
    Properties props = new Properties();

    return getStockLocalClient(port, props);
  }

  public static NettyAvroRpcClient getStockLocalClient(int port, Properties starterProp) {
    starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    starterProp.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
        "127.0.0.1" + ":" + port);
    NettyAvroRpcClient client = new NettyAvroRpcClient();
    client.configure(starterProp);

    return client;
  }

  /**
   * Start a NettyServer, wait a moment for it to spin up, and return it.
   */
  public static Server startServer(AvroSourceProtocol handler, int port, boolean enableCompression) {
    Responder responder = new SpecificResponder(AvroSourceProtocol.class, handler);
    Server server = null;
    try {
      if (enableCompression) {
        server = new NettyServer(responder, new InetSocketAddress(localhost, port),
                (ch) -> {
                  ChannelPipeline pipeline = ch.pipeline();
                  ZlibEncoder encoder = new JZlibEncoder(6);
                  pipeline.addFirst("deflater", encoder);
                  pipeline.addFirst("inflater", new JZlibDecoder());
                });
      } else {
        server = new NettyServer(responder, new InetSocketAddress(localhost, port));
      }
      server.start();
      logger.info("Server started on hostname: {}, port: {}",
              new Object[]{localhost, Integer.toString(server.getPort())});
      Thread.sleep(300L);
    } catch (InterruptedException ex) {
      logger.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }

    return server;
  }

  public static Server startServer(AvroSourceProtocol handler) {
    return startServer(handler, 0, false);
  }

  public static Server startServer(AvroSourceProtocol handler, int port) {
    return startServer(handler, port, false);
  }


  /**
   * Request that the specified Server stop, and attempt to wait for it to exit.
   * @param server A running NettyServer
   */
  public static void stopServer(Server server) {
    try {
      int port = server.getPort();
      server.close();
      server.join();
      logger.info("Server stopped on port: {}", port);
    } catch (InterruptedException ex) {
      logger.error("Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }
  }

  public static class LoadBalancedAvroHandler implements AvroSourceProtocol {

    private int appendCount = 0;
    private int appendBatchCount = 0;

    private boolean failed = false;

    public int getAppendCount() {
      return appendCount;
    }

    public int getAppendBatchCount() {
      return appendBatchCount;
    }

    public boolean isFailed() {
      return failed;
    }

    public void setFailed() {
      this.failed = true;
    }

    public void setOK() {
      this.failed = false;
    }

    @Override
    public Status append(AvroFlumeEvent event) {
      if (failed) {
        logger.debug("Event rejected");
        return Status.FAILED;
      }
      logger.debug("LB: Received event from append(): {}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      appendCount++;
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      if (failed) {
        logger.debug("Event batch rejected");
        return Status.FAILED;
      }
      logger.debug("LB: Received {} events from appendBatch()",
          events.size());

      appendBatchCount++;
      return Status.OK;
    }
  }

  /**
   * A service that logs receipt of the request and returns OK
   */
  public static class OKAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) {
      logger.info("OK: Received event from append(): {}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      logger.info("OK: Received {} events from appendBatch()", events.size());
      return Status.OK;
    }

  }

  /**
   * A service that logs receipt of the request and returns Failed
   */
  public static class FailedAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) {
      logger.info("Failed: Received event from append(): {}",
                  new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.FAILED;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      logger.info("Failed: Received {} events from appendBatch()", events.size());
      return Status.FAILED;
    }

  }

  /**
   * A service that logs receipt of the request and returns Unknown
   */
  public static class UnknownAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) {
      logger.info("Unknown: Received event from append(): {}",
                  new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.UNKNOWN;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      logger.info("Unknown: Received {} events from appendBatch()", events.size());
      return Status.UNKNOWN;
    }
  }

  /**
   * A service that logs receipt of the request and then throws an exception
   */
  public static class ThrowingAvroHandler implements AvroSourceProtocol.Callback {

    @Override
    public void append(AvroFlumeEvent event, org.apache.avro.ipc.Callback<Status> callback)
            throws java.io.IOException {
      logger.info("Throwing: Received event from append(): {}",
                  new String(event.getBody().array(), Charset.forName("UTF8")));
      throw new java.io.IOException("Handler smash!");
    }

    @Override
    public Status append(AvroFlumeEvent event) {
      logger.info("Throwing unavailable: Received event from append(): {}",
             new String(event.getBody().array(), Charset.forName("UTF8")));
      return null;
    }

    @Override
    public void appendBatch(List<AvroFlumeEvent> events, org.apache.avro.ipc.Callback<Status> callback)
            throws java.io.IOException {
      logger.info("Throwing: Received {} events from appendBatch()", events.size());
      throw new java.io.IOException("Handler smash!");
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      logger.info("Throwing unavailable: Received {} events from appendBatch()", events.size());
      return null;
    }
  }
}
