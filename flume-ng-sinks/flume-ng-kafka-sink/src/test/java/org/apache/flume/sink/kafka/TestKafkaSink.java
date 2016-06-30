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

import com.google.common.base.Charsets;
import kafka.message.MessageAndMetadata;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flume.sink.kafka.KafkaSinkConstants.AVRO_EVENT;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BROKER_LIST_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_KEY_SERIALIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PRODUCER_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.OLD_BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_CONFIG;
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
    topics.add(DEFAULT_TOPIC);
    topics.add(TestConstants.STATIC_TOPIC);
    topics.add(TestConstants.CUSTOM_TOPIC);
    testUtil.initTopicList(topics);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testKafkaProperties() {

    KafkaSink kafkaSink = new KafkaSink();
    Context context = new Context();
    context.put(KAFKA_PREFIX + TOPIC_CONFIG, "");
    context.put(KAFKA_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "override.default.serializer");
    context.put("kafka.producer.fake.property", "kafka.property.value");
    context.put("kafka.bootstrap.servers", "localhost:9092,localhost:9092");
    context.put("brokerList", "real-broker-list");
    Configurables.configure(kafkaSink, context);

    Properties kafkaProps = kafkaSink.getKafkaProps();

    //check that we have defaults set
    assertEquals(kafkaProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
                 DEFAULT_KEY_SERIALIZER);
    //check that kafka properties override the default and get correct name
    assertEquals(kafkaProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
                 "override.default.serializer");
    //check that any kafka-producer property gets in
    assertEquals(kafkaProps.getProperty("fake.property"),
                 "kafka.property.value");
    //check that documented property overrides defaults
    assertEquals(kafkaProps.getProperty("bootstrap.servers"),
                 "localhost:9092,localhost:9092");
  }

  @Test
  public void testOldProperties() {
    KafkaSink kafkaSink = new KafkaSink();
    Context context = new Context();
    context.put("topic", "test-topic");
    context.put(OLD_BATCH_SIZE, "300");
    context.put(BROKER_LIST_FLUME_KEY, "localhost:9092,localhost:9092");
    context.put(REQUIRED_ACKS_FLUME_KEY, "all");
    Configurables.configure(kafkaSink, context);

    Properties kafkaProps = kafkaSink.getKafkaProps();

    assertEquals(kafkaSink.getTopic(), "test-topic");
    assertEquals(kafkaSink.getBatchSize(), 300);
    assertEquals(kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                 "localhost:9092,localhost:9092");
    assertEquals(kafkaProps.getProperty(ProducerConfig.ACKS_CONFIG), "all");

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

    String fetchedMsg = new String((byte[]) testUtil.getNextMessageFromConsumer(DEFAULT_TOPIC)
                                                    .message());
    assertEquals(msg, fetchedMsg);
  }

  @Test
  public void testStaticTopic() {
    Context context = prepareDefaultContext();
    // add the static topic
    context.put(TOPIC_CONFIG, TestConstants.STATIC_TOPIC);
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

  @SuppressWarnings("rawtypes")
  @Test
  public void testAvroEvent() throws IOException {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    context.put(AVRO_EVENT, "true");
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-avro-event";

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", TestConstants.CUSTOM_TOPIC);
    headers.put("key", TestConstants.CUSTOM_KEY);
    headers.put(TestConstants.HEADER_1_KEY, TestConstants.HEADER_1_VALUE);
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

    MessageAndMetadata fetchedMsg = testUtil.getNextMessageFromConsumer(TestConstants.CUSTOM_TOPIC);

    ByteArrayInputStream in = new ByteArrayInputStream((byte[]) fetchedMsg.message());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    SpecificDatumReader<AvroFlumeEvent> reader =
        new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);

    AvroFlumeEvent avroevent = reader.read(null, decoder);

    String eventBody = new String(avroevent.getBody().array(), Charsets.UTF_8);
    Map<CharSequence, CharSequence> eventHeaders = avroevent.getHeaders();

    assertEquals(msg, eventBody);
    assertEquals(TestConstants.CUSTOM_KEY, new String((byte[]) fetchedMsg.key(), "UTF-8"));

    assertEquals(TestConstants.HEADER_1_VALUE,
                 eventHeaders.get(new Utf8(TestConstants.HEADER_1_KEY)).toString());
    assertEquals(TestConstants.CUSTOM_KEY, eventHeaders.get(new Utf8("key")).toString());
  }

  @Test
  public void testEmptyChannel() throws UnsupportedEncodingException, EventDeliveryException {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    Sink.Status status = kafkaSink.process();
    if (status != Sink.Status.BACKOFF) {
      fail("Error Occurred");
    }
    assertNull(testUtil.getNextMessageFromConsumer(DEFAULT_TOPIC));
  }

  private Context prepareDefaultContext() {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS_CONFIG, testUtil.getKafkaServerUrl());
    context.put(BATCH_SIZE, "1");
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
