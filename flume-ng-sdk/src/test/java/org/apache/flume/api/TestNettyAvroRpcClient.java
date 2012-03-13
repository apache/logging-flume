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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;

import org.apache.flume.api.RpcTestUtils.FailedAvroHandler;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.apache.flume.api.RpcTestUtils.ThrowingAvroHandler;
import org.apache.flume.api.RpcTestUtils.UnknownAvroHandler;
import org.junit.Assert;

/**
 *
 */
public class TestNettyAvroRpcClient {

  private static final Logger logger =
      Logger.getLogger(TestNettyAvroRpcClient.class.getName());

  private static final String localhost = "localhost";

  /**
   * Simple request
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test
  public void testOKServerSimple() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerSimpleAppendTest(new OKAvroHandler());
  }

  /**
   * Simple batch request
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test
  public void testOKServerBatch() throws FlumeException,
      EventDeliveryException {
    RpcTestUtils.handlerBatchAppendTest(new OKAvroHandler());
  }

  /**
   * Try to connect to a closed port.
   * Note: this test tries to connect to port 1 on localhost.
   * @throws FlumeException
   */
  @Test(expected=FlumeException.class)
  public void testUnableToConnect() throws FlumeException {
    NettyAvroRpcClient client = new NettyAvroRpcClient.Builder()
        .hostname(localhost).port(1).build();
  }

  /**
   * Send too many events at once. Should handle this case gracefully.
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test
  public void testBatchOverrun() throws FlumeException, EventDeliveryException {

    int batchSize = 10;
    int moreThanBatchSize = batchSize + 1;
    NettyAvroRpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = new NettyAvroRpcClient.Builder()
          .hostname(localhost).port(server.getPort()).batchSize(batchSize)
          .build();

      // send one more than the batch size
      List<Event> events = new ArrayList<Event>();
      for (int i = 0; i < moreThanBatchSize; i++) {
        events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
      }
      client.appendBatch(events);
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * First connect the client, then shut down the server, then send a request.
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test(expected=EventDeliveryException.class)
  public void testServerDisconnect() throws FlumeException,
      EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcTestUtils.getStockLocalClient(server.getPort());
      server.close();
      try {
        server.join();
      } catch (InterruptedException ex) {
        logger.log(Level.WARNING, "Thread interrupted during join()", ex);
        Thread.currentThread().interrupt();
      }
      try {
        client.append(EventBuilder.withBody("hello", Charset.forName("UTF8")));
      } finally {
        Assert.assertFalse("Client should not be active", client.isActive());
      }
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * First connect the client, then close the client, then send a request.
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test(expected=EventDeliveryException.class)
  public void testClientClosedRequest() throws FlumeException,
      EventDeliveryException {
    NettyAvroRpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcTestUtils.getStockLocalClient(server.getPort());
      client.close();
      Assert.assertFalse("Client should not be active", client.isActive());
      System.out.println("Yaya! I am not active after client close!");
      client.append(EventBuilder.withBody("hello", Charset.forName("UTF8")));
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  /**
   * Send an event to an online server that returns FAILED.
   */
  @Test(expected=EventDeliveryException.class)
  public void testFailedServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new FailedAvroHandler());
    logger.severe("Failed: I should never have gotten here!");
  }

  /**
   * Send an event to an online server that returns UNKNOWN.
   */
  @Test(expected=EventDeliveryException.class)
  public void testUnknownServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new UnknownAvroHandler());
    logger.severe("Unknown: I should never have gotten here!");
  }

  /**
   * Send an event to an online server that throws an exception.
   */
  @Test(expected=EventDeliveryException.class)
  public void testThrowingServerSimple() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerSimpleAppendTest(new ThrowingAvroHandler());
    logger.severe("Throwing: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that returns FAILED.
   */
  @Test(expected=EventDeliveryException.class)
  public void testFailedServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new FailedAvroHandler());
    logger.severe("Failed: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that returns UNKNOWN.
   */
  @Test(expected=EventDeliveryException.class)
  public void testUnknownServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new UnknownAvroHandler());
    logger.severe("Unknown: I should never have gotten here!");
  }

  /**
   * Send a batch of events to a server that always throws exceptions.
   */
  @Test(expected=EventDeliveryException.class)
  public void testThrowingServerBatch() throws FlumeException,
      EventDeliveryException {

    RpcTestUtils.handlerBatchAppendTest(new ThrowingAvroHandler());
    logger.severe("Throwing: I should never have gotten here!");
  }

}
