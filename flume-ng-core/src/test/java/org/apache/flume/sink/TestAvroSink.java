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

package org.apache.flume.sink;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Charsets;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSink.class);
  private static final String hostname = "127.0.0.1";
  private static final Integer port = 41414;

  private AvroSink sink;
  private Channel channel;

  @Before
  public void setUp() {
    sink = new AvroSink();
    channel = new MemoryChannel();

    Context context = new Context();

    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));

    sink.setChannel(channel);

    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
  }

  @Test
  public void testLifecycle() throws InterruptedException,
      InstantiationException, IllegalAccessException {
    Server server = createServer(new MockAvroServer());

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testProcess() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {

    Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
    Server server = createServer(new MockAvroServer());

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testTimeout() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    Event event = EventBuilder.withBody("foo", Charsets.UTF_8);
    AtomicLong delay = new AtomicLong();
    Server server = createServer(new DelayMockAvroServer(delay));
    server.start();
    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 4; i++) {
      channel.put(event);
    }
    txn.commit();
    txn.close();

    // should throw EventDeliveryException due to connect timeout
    delay.set(3000L); // because connect-timeout = 2000
    boolean threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to connect timeout. Exception follows.",
          ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to connect timeout", threw);

    // now, allow the connect handshake to occur
    delay.set(0);
    sink.process();

    // should throw another EventDeliveryException due to request timeout
    delay.set(4000L); // because request-timeout = 3000
    threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to request timeout. Exception follows.",
          ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to request timeout", threw);

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));
    server.close();
  }

  @Test
  public void testFailedConnect() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {

    Event event = EventBuilder.withBody("test event 1",
        Charset.forName("UTF8"));
    Server server = createServer(new MockAvroServer());

    server.start();
    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Thread.sleep(500L); // let socket startup
    server.close();
    Thread.sleep(500L); // sleep a little to allow close occur

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 5; i++) {
      boolean threwException = false;
      try {
        sink.process();
      } catch (EventDeliveryException e) {
        threwException = true;
      }
      Assert.assertTrue("Must throw EventDeliveryException if disconnected",
          threwException);
    }

    server = createServer(new MockAvroServer());
    server.start();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));
    server.close();
  }

  private Server createServer(AvroSourceProtocol protocol)
      throws IllegalAccessException, InstantiationException {
    Server server = new NettyServer(new SpecificResponder(
        AvroSourceProtocol.class, protocol), new InetSocketAddress(
        hostname, port));

    return server;
  }

  private static class MockAvroServer implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.debug("Received event:{}", event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {
      logger.debug("Received event batch:{}", events);
      return Status.OK;
    }

  }

  private static class DelayMockAvroServer implements AvroSourceProtocol {

    private final AtomicLong delay;

    public DelayMockAvroServer(AtomicLong delay) {
      this.delay = delay;
    }

    private void sleep() throws AvroRemoteException {
      try {
        Thread.sleep(delay.get());
      } catch (InterruptedException e) {
        throw new AvroRemoteException("Interrupted while sleeping", e);
      }
    }

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.debug("Received event:{}; delaying for {}ms", event, delay);
      sleep();
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {
      logger.debug("Received event batch:{}; delaying for {}ms", events, delay);
      sleep();
      return Status.OK;
    }

  }

}
