/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka;

import kafka.message.MessageAndMetadata;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests for Kafka Sink
 */
public class TestKafkaSink {

  private static TestUtil testUtil = TestUtil.getInstance();

  @BeforeClass
  public static void setup() {
    testUtil.prepare();
    List<String> topics = new ArrayList<String>(3);
    topics.add(KafkaSinkConstants.DEFAULT_TOPIC);
    topics.add(TestConstants.STATIC_TOPIC);
    topics.add(TestConstants.CUSTOM_TOPIC);
    testUtil.initTopicList(topics);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testDefaultTopic() {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "default-topic-test";
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes());
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    String fetchedMsg = new String((byte[])
      testUtil.getNextMessageFromConsumer(KafkaSinkConstants.DEFAULT_TOPIC)
        .message());
    assertEquals(msg, fetchedMsg);
  }

  @Test
  public void testStaticTopic() {
    Context context = prepareDefaultContext();
    // add the static topic
    context.put(KafkaSinkConstants.TOPIC, TestConstants.STATIC_TOPIC);
    String msg = "static-topic-test";

    try {
      Sink.Status status = prepareAndSend(context, msg);
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    String fetchedMsg = new String((byte[]) testUtil.getNextMessageFromConsumer(
      TestConstants.STATIC_TOPIC).message());
    assertEquals(msg, fetchedMsg);
  }

  @Test
  public void testTopicAndKeyFromHeader() throws UnsupportedEncodingException {


    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-topic-and-key-from-header";
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", TestConstants.CUSTOM_TOPIC);
    headers.put("key", TestConstants.CUSTOM_KEY);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    MessageAndMetadata fetchedMsg =
      testUtil.getNextMessageFromConsumer(TestConstants.CUSTOM_TOPIC);

    assertEquals(msg, new String((byte[]) fetchedMsg.message(), "UTF-8"));
    assertEquals(TestConstants.CUSTOM_KEY,
      new String((byte[]) fetchedMsg.key(), "UTF-8"));

  }

  @Test
  public void testEmptyChannel() throws UnsupportedEncodingException,
          EventDeliveryException {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    Sink.Status status = kafkaSink.process();
    if (status == Sink.Status.BACKOFF) {
      fail("Error Occurred");
    }
    assertNull(
      testUtil.getNextMessageFromConsumer(KafkaSinkConstants.DEFAULT_TOPIC));
  }

  private Context prepareDefaultContext() {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put("brokerList", testUtil.getKafkaServerUrl());
    context.put("kafka.request.required.acks", "1");
    context.put("kafka.producer.type","sync");
    context.put("batchSize", "1");
    return context;
  }

  private Sink.Status prepareAndSend(Context context, String msg)
    throws EventDeliveryException {
    Sink kafkaSink = new KafkaSink();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes());
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    return kafkaSink.process();
  }

}
