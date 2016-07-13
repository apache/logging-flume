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

package org.apache.flume.source.avroLegacy;

import com.cloudera.flume.handlers.avro.AvroFlumeOGEvent;
import com.cloudera.flume.handlers.avro.FlumeOGEventAvroServer;
import com.cloudera.flume.handlers.avro.Priority;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
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
import org.jboss.netty.channel.ChannelException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestLegacyAvroSource {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory
      .getLogger(TestLegacyAvroSource.class);

  private int selectedPort;
  private AvroLegacySource source;
  private Channel channel;

  @Before
  public void setUp() {
    source = new AvroLegacySource();
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
        context.put("host", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        // Assume port in use, try another one
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
  public void testRequest() throws InterruptedException, IOException {
    boolean bound = false;
    int i;

    for (i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("host", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        // Assume port in use, try another one
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    // setup a requester, to send a flume OG event
    URL url = new URL("http", "0.0.0.0", selectedPort, "/");
    Transceiver http = new HttpTransceiver(url);
    FlumeOGEventAvroServer client = SpecificRequestor.getClient(
        FlumeOGEventAvroServer.class, http);

    AvroFlumeOGEvent avroEvent = AvroFlumeOGEvent.newBuilder().setHost("foo")
        .setPriority(Priority.INFO).setNanos(0).setTimestamp(1)
        .setFields(new HashMap<CharSequence, ByteBuffer>())
        .setBody(ByteBuffer.wrap("foo".getBytes())).build();

    client.append(avroEvent);

    // check if the even has arrived in the channel through OG avro source
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "foo",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();

    source.stop();

    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

}
