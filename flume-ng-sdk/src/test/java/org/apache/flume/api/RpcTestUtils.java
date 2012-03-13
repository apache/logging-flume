/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.api;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;

/**
 * Helpers for Netty Avro RPC testing
 */
public class RpcTestUtils {

  private static final Logger logger =
      Logger.getLogger(TestNettyAvroRpcClient.class.getName());

  private static final String localhost = "localhost";


  /**
   * Helper method for testing simple (single) appends on handlers
   * @param handler
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  public static void handlerSimpleAppendTest(AvroSourceProtocol handler)
      throws FlumeException, EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = startServer(handler);
    try {
      client = getStockLocalClient(server.getPort());
      boolean isActive = client.isActive();
      Assert.assertTrue("Client should be active", isActive);
      client.append(EventBuilder.withBody("wheee!!!", Charset.forName("UTF8")));
    } finally {
      stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * Helper method for testing batch appends on handlers
   * @param handler
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  public static void handlerBatchAppendTest(AvroSourceProtocol handler)
      throws FlumeException, EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = startServer(handler);
    try {
      client = getStockLocalClient(server.getPort());
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
    NettyAvroRpcClient client = new NettyAvroRpcClient.Builder()
        .hostname(localhost).port(port).build();

    return client;
  }

  /**
   * Start a NettyServer, wait a moment for it to spin up, and return it.
   */
  public static Server startServer(AvroSourceProtocol handler) {
    Responder responder = new SpecificResponder(AvroSourceProtocol.class,
        handler);
    Server server = new NettyServer(responder,
        new InetSocketAddress(localhost, 0));
    server.start();
    logger.log(Level.INFO, "Server started on hostname: {0}, port: {1}",
        new Object[] { localhost, Integer.toString(server.getPort()) });

    try {

      Thread.sleep(300L);

    } catch (InterruptedException ex) {
      logger.log(Level.SEVERE, "Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }

    return server;
  }

  /**
   * Request that the specified Server stop, and attempt to wait for it to exit.
   * @param server A running NettyServer
   */
  public static void stopServer(Server server) {
    try {
      server.close();
      server.join();
    } catch (InterruptedException ex) {
      logger.log(Level.SEVERE, "Thread interrupted. Exception follows.", ex);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A service that logs receipt of the request and returns OK
   */
  public static class OKAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.log(Level.INFO, "OK: Received event from append(): {0}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
        AvroRemoteException {
      logger.log(Level.INFO, "OK: Received {0} events from appendBatch()",
          events.size());
      return Status.OK;
    }

  }

  /**
   * A service that logs receipt of the request and returns Failed
   */
  public static class FailedAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.log(Level.INFO, "Failed: Received event from append(): {0}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.FAILED;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
        AvroRemoteException {
      logger.log(Level.INFO, "Failed: Received {0} events from appendBatch()",
          events.size());
      return Status.FAILED;
    }

  }

  /**
   * A service that logs receipt of the request and returns Unknown
   */
  public static class UnknownAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.log(Level.INFO, "Unknown: Received event from append(): {0}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      return Status.UNKNOWN;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
        AvroRemoteException {
      logger.log(Level.INFO, "Unknown: Received {0} events from appendBatch()",
          events.size());
      return Status.UNKNOWN;
    }

  }

  /**
   * A service that logs receipt of the request and then throws an exception
   */
  public static class ThrowingAvroHandler implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.log(Level.INFO, "Throwing: Received event from append(): {0}",
          new String(event.getBody().array(), Charset.forName("UTF8")));
      throw new AvroRemoteException("Handler smash!");
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
        AvroRemoteException {
      logger.log(Level.INFO, "Throwing: Received {0} events from appendBatch()",
          events.size());
      throw new AvroRemoteException("Handler smash!");
    }

  }

}
