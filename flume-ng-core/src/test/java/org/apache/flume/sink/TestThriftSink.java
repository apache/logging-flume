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

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.api.ThriftTestingSource;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TestThriftSink {
  private ThriftTestingSource src;
  private ThriftSink sink;
  private MemoryChannel channel;
  private String hostname;
  private int port;

  private final Random random = new Random();

  @Before
  public void setUp() throws Exception {
    sink = new ThriftSink();
    channel = new MemoryChannel();
    hostname = "0.0.0.0";
    port = random.nextInt(50000) + 1024;
    Context context = new Context();

    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("request-timeout", String.valueOf(2000L));

    sink.setChannel(channel);

    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
  }

  @After
  public void tearDown() throws Exception {
    channel.stop();
    sink.stop();
    src.stop();
  }

  @Test
  public void testProcess() throws Exception {

    Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.OK.name(),
      port);

    channel.start();
    sink.start();

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 11; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();
    for (int i = 0; i < 6; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertEquals(11, src.flumeEvents.size());
    Assert.assertEquals(6, src.batchCount);
    Assert.assertEquals(0, src.individualCount);

  }

  @Test
  public void testTimeout() throws Exception {
    AtomicLong delay = new AtomicLong();
    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.ALTERNATE
      .name(), port);
    src.setDelay(delay);
    delay.set(2500);

    Event event = EventBuilder.withBody("foo", Charsets.UTF_8);
    sink.start();

    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 4; i++) {
      channel.put(event);
    }
    txn.commit();
    txn.close();

    // should throw EventDeliveryException due to connect timeout
    boolean threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      threw = true;
    }

    Assert.assertTrue("Must throw due to connect timeout", threw);

    // now, allow the connect handshake to occur
    delay.set(0);
    sink.process();

    // should throw another EventDeliveryException due to request timeout
    delay.set(2500L); // because request-timeout = 3000
    threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      threw = true;
    }

    Assert.assertTrue("Must throw due to request timeout", threw);

    sink.stop();
  }

  @Test
  public void testFailedConnect() throws Exception {

    Event event = EventBuilder.withBody("test event 1",
      Charset.forName("UTF8"));

    sink.start();

    Thread.sleep(500L); // let socket startup
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

    src = new ThriftTestingSource(ThriftTestingSource.HandlerType.OK.name(),
      port);

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());
    sink.stop();
  }
}
