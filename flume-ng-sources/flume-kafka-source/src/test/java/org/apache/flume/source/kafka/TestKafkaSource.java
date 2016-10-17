/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.source.kafka;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import kafka.common.TopicExistsException;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flume.source.kafka.KafkaSourceConstants.AVRO_EVENT;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_DURATION_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_SIZE;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_AUTO_COMMIT;
import static org.apache.flume.source.kafka.KafkaSourceConstants.KAFKA_CONSUMER_PREFIX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.OLD_GROUP_ID;
import static org.apache.flume.source.kafka.KafkaSourceConstants.PARTITION_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TIMESTAMP_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS_REGEX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestKafkaSource {
  private static final Logger log = LoggerFactory.getLogger(TestKafkaSource.class);

  private KafkaSource kafkaSource;
  private KafkaSourceEmbeddedKafka kafkaServer;
  private Context context;
  private List<Event> events;

  private final Set<String> usedTopics = new HashSet<String>();
  private String topic0 = "test1";
  private String topic1 = "topic1";

  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    kafkaSource = new KafkaSource();
    kafkaServer = new KafkaSourceEmbeddedKafka(null);
    try {
      kafkaServer.createTopic(topic0, 1);
      usedTopics.add(topic0);
      kafkaServer.createTopic(topic1, 3);
      usedTopics.add(topic1);
    } catch (TopicExistsException e) {
      //do nothing
      e.printStackTrace();
    }
    context = prepareDefaultContext("flume-group");
    kafkaSource.setChannelProcessor(createGoodChannel());
  }

  private Context prepareDefaultContext(String groupId) {
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS, kafkaServer.getBootstrapServers());
    context.put(KAFKA_CONSUMER_PREFIX + "group.id", groupId);
    return context;
  }

  @After
  public void tearDown() throws Exception {
    kafkaSource.stop();
    kafkaServer.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOffsets() throws InterruptedException, EventDeliveryException {
    long batchDuration = 2000;
    context.put(TOPICS, topic1);
    context.put(BATCH_DURATION_MS,
            String.valueOf(batchDuration));
    context.put(BATCH_SIZE, "3");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);
    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
    assertEquals(0, events.size());
    kafkaServer.produce(topic1, "", "record1");
    kafkaServer.produce(topic1, "", "record2");
    Thread.sleep(500L);
    status = kafkaSource.process();
    assertEquals(Status.READY, status);
    assertEquals(2, events.size());
    events.clear();
    kafkaServer.produce(topic1, "", "record3");
    kafkaServer.produce(topic1, "", "record4");
    kafkaServer.produce(topic1, "", "record5");
    Thread.sleep(500L);
    assertEquals(Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record3", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record4", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record5", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record6");
    kafkaServer.produce(topic1, "", "record7");
    kafkaServer.produce(topic1, "", "record8");
    kafkaServer.produce(topic1, "", "record9");
    kafkaServer.produce(topic1, "", "record10");
    Thread.sleep(500L);
    assertEquals(Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record6", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record7", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record8", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record11");
    // status must be READY due to time out exceed.
    assertEquals(Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record9", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record10", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record11", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record12");
    kafkaServer.produce(topic1, "", "record13");
    // stop kafka source
    kafkaSource.stop();
    // start again
    kafkaSource = new KafkaSource();
    kafkaSource.setChannelProcessor(createGoodChannel());
    kafkaSource.configure(context);
    kafkaSource.start();
    kafkaServer.produce(topic1, "", "record14");
    Thread.sleep(1000L);
    assertEquals(Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record12", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record13", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record14", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmpty() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");

    Thread.sleep(500L);
    Assert.assertEquals(Status.READY, kafkaSource.process());
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());
    Assert.assertEquals(1, events.size());

    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
            Charsets.UTF_8));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmptyBatch() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"2");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");
    kafkaServer.produce(topic0, "", "foo, bar");

    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.READY, status);
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
            Charsets.UTF_8));
    Assert.assertEquals("foo, bar", new String(events.get(1).getBody(),
            Charsets.UTF_8));

  }


  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItEmpty() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNonExistingTopic() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(TOPICS,"faketopic");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
  }

  @SuppressWarnings("unchecked")
  @Test(expected = FlumeException.class)
  public void testNonExistingKafkaServer() throws EventDeliveryException, SecurityException,
                                                  NoSuchFieldException, IllegalArgumentException,
                                                  IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BOOTSTRAP_SERVERS,"blabla:666");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
  }

  @Test
  public void testBatchTime() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_DURATION_MS, "250");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    for (int i = 1; i < 5000; i++) {
      kafkaServer.produce(topic0, "", "hello, world " + i);
    }
    Thread.sleep(500L);

    long error = 50;
    long startTime = System.currentTimeMillis();
    Status status = kafkaSource.process();
    long endTime = System.currentTimeMillis();
    assertEquals(Status.READY, status);
    assertTrue(endTime - startTime < (context.getLong(BATCH_DURATION_MS) + error));
  }

  // Consume event, stop source, start again and make sure we are not
  // consuming same event again
  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");

    Thread.sleep(500L);

    Assert.assertEquals(Status.READY, kafkaSource.process());
    kafkaSource.stop();
    Thread.sleep(500L);
    kafkaSource.start();
    Thread.sleep(500L);
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());
  }

  // Remove channel processor and test if we can consume events again
  @Test
  public void testNonCommit() throws EventDeliveryException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS,"30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");
    Thread.sleep(500L);

    kafkaSource.setChannelProcessor(createBadChannel());
    log.debug("processing from kafka to bad channel");
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());

    log.debug("repairing channel");
    kafkaSource.setChannelProcessor(createGoodChannel());

    log.debug("re-process to good channel - this should work");
    kafkaSource.process();
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @Test
  public void testTwoBatches() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS, "30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(), Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topic0, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @Test
  public void testTwoBatchesWithAutocommit() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS,"30000");
    context.put(KAFKA_CONSUMER_PREFIX + "enable.auto.commit", "true");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(), Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topic0, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullKey() throws EventDeliveryException, SecurityException, NoSuchFieldException,
                                   IllegalArgumentException, IllegalAccessException,
                                   InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, null, "hello, world");

    Thread.sleep(500L);

    Assert.assertEquals(Status.READY, kafkaSource.process());
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());
    Assert.assertEquals(1, events.size());

    Assert.assertEquals("hello, world", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @Test
  public void testSourceProperties() {
    Context context = new Context();
    context.put(TOPICS, "test1, test2");
    context.put(TOPICS_REGEX, "^stream[0-9]$");
    context.put(BOOTSTRAP_SERVERS, "bootstrap-servers-list");
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);

    //check that kafka.topics.regex has higher priority than topics
    //type of subscriber should be PatternSubscriber
    KafkaSource.Subscriber<Pattern> subscriber = source.getSubscriber();
    Pattern pattern = subscriber.get();
    Assert.assertTrue(pattern.matcher("stream1").find());
  }

  @Test
  public void testKafkaProperties() {
    Context context = new Context();
    context.put(TOPICS, "test1, test2");
    context.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG,
                "override.default.group.id");
    context.put(KAFKA_CONSUMER_PREFIX + "fake.property", "kafka.property.value");
    context.put(BOOTSTRAP_SERVERS, "real-bootstrap-servers-list");
    context.put(KAFKA_CONSUMER_PREFIX + "bootstrap.servers", "bad-bootstrap-servers-list");
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);
    Properties kafkaProps = source.getConsumerProps();

    //check that we have defaults set
    assertEquals(String.valueOf(DEFAULT_AUTO_COMMIT),
                 kafkaProps.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    //check that kafka properties override the default and get correct name
    assertEquals("override.default.group.id",
                 kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    //check that any kafka property gets in
    assertEquals("kafka.property.value",
                 kafkaProps.getProperty("fake.property"));
    //check that documented property overrides defaults
    assertEquals("real-bootstrap-servers-list",
                 kafkaProps.getProperty("bootstrap.servers"));
  }

  @Test
  public void testOldProperties() {
    Context context = new Context();

    context.put(TOPIC, "old.topic");
    context.put(OLD_GROUP_ID, "old.groupId");
    context.put(BOOTSTRAP_SERVERS, "real-bootstrap-servers-list");
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);
    Properties kafkaProps = source.getConsumerProps();

    KafkaSource.Subscriber<List<String>> subscriber = source.getSubscriber();
    //check topic was set
    assertEquals("old.topic", subscriber.get().get(0));
    //check that kafka old properties override the default and get correct name
    assertEquals("old.groupId", kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));

    source = new KafkaSource();
    context.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, "override.old.group.id");
    source.doConfigure(context);
    kafkaProps = source.getConsumerProps();
    //check that kafka new properties override old
    assertEquals("override.old.group.id", kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));

    context.clear();
    context.put(BOOTSTRAP_SERVERS, "real-bootstrap-servers-list");
    context.put(TOPIC, "old.topic");
    source = new KafkaSource();
    source.doConfigure(context);
    kafkaProps = source.getConsumerProps();
    //check defaults set
    assertEquals(KafkaSourceConstants.DEFAULT_GROUP_ID,
                 kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
  }

  @Test
  public void testPatternBasedSubscription() {
    Context context = new Context();

    context.put(TOPICS_REGEX, "^topic[0-9]$");
    context.put(OLD_GROUP_ID, "old.groupId");
    context.put(BOOTSTRAP_SERVERS, "real-bootstrap-servers-list");
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);
    KafkaSource.Subscriber<Pattern> subscriber = source.getSubscriber();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(subscriber.get().matcher("topic" + i).find());
    }
    Assert.assertFalse(subscriber.get().matcher("topic").find());
  }

  @Test
  public void testAvroEvent() throws InterruptedException, EventDeliveryException, IOException {
    SpecificDatumWriter<AvroFlumeEvent> writer;
    ByteArrayOutputStream tempOutStream;
    BinaryEncoder encoder;
    byte[] bytes;

    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    context.put(AVRO_EVENT, "true");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    tempOutStream = new ByteArrayOutputStream();
    writer = new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class);

    Map<CharSequence, CharSequence> headers = new HashMap<CharSequence,CharSequence>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");

    AvroFlumeEvent e = new AvroFlumeEvent(headers, ByteBuffer.wrap("hello, world".getBytes()));
    encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream, null);
    writer.write(e, encoder);
    encoder.flush();
    bytes = tempOutStream.toByteArray();

    kafkaServer.produce(topic0, "", bytes);

    String currentTimestamp = Long.toString(System.currentTimeMillis());

    headers.put(TIMESTAMP_HEADER, currentTimestamp);
    headers.put(PARTITION_HEADER, "1");
    headers.put(TOPIC_HEADER, "topic0");

    e = new AvroFlumeEvent(headers, ByteBuffer.wrap("hello, world2".getBytes()));
    tempOutStream.reset();
    encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream, null);
    writer.write(e, encoder);
    encoder.flush();
    bytes = tempOutStream.toByteArray();

    kafkaServer.produce(topic0, "", bytes);

    Thread.sleep(500L);
    Assert.assertEquals(Status.READY, kafkaSource.process());
    Assert.assertEquals(Status.READY, kafkaSource.process());
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());

    Assert.assertEquals(2, events.size());

    Event event = events.get(0);

    Assert.assertEquals("hello, world", new String(event.getBody(), Charsets.UTF_8));

    Assert.assertEquals("value1", e.getHeaders().get("header1"));
    Assert.assertEquals("value2", e.getHeaders().get("header2"));


    event = events.get(1);

    Assert.assertEquals("hello, world2", new String(event.getBody(), Charsets.UTF_8));

    Assert.assertEquals("value1", e.getHeaders().get("header1"));
    Assert.assertEquals("value2", e.getHeaders().get("header2"));
    Assert.assertEquals(currentTimestamp, e.getHeaders().get(TIMESTAMP_HEADER));
    Assert.assertEquals(e.getHeaders().get(PARTITION_HEADER), "1");
    Assert.assertEquals(e.getHeaders().get(TOPIC_HEADER),"topic0");

  }

  @Test
  public void testBootstrapLookup() {
    Context context = new Context();

    context.put(ZOOKEEPER_CONNECT_FLUME_KEY, kafkaServer.getZkConnectString());
    context.put(TOPIC, "old.topic");
    context.put(OLD_GROUP_ID, "old.groupId");
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);
    String bootstrapServers = source.getBootstrapServers();
    Assert.assertEquals(kafkaServer.getBootstrapServers(), bootstrapServers);
  }

  @Test
  public void testMigrateOffsetsNone() throws Exception {
    doTestMigrateZookeeperOffsets(false, false, "testMigrateOffsets-none");
  }

  @Test
  public void testMigrateOffsetsZookeeper() throws Exception {
    doTestMigrateZookeeperOffsets(true, false, "testMigrateOffsets-zookeeper");
  }

  @Test
  public void testMigrateOffsetsKafka() throws Exception {
    doTestMigrateZookeeperOffsets(false, true, "testMigrateOffsets-kafka");
  }

  @Test
  public void testMigrateOffsetsBoth() throws Exception {
    doTestMigrateZookeeperOffsets(true, true, "testMigrateOffsets-both");
  }

  public void doTestMigrateZookeeperOffsets(boolean hasZookeeperOffsets, boolean hasKafkaOffsets,
                                            String group) throws Exception {
    // create a topic with 1 partition for simplicity
    String topic = findUnusedTopic();
    kafkaServer.createTopic(topic, 1);

    Context context = prepareDefaultContext(group);
    context.put(ZOOKEEPER_CONNECT_FLUME_KEY, kafkaServer.getZkConnectString());
    context.put(TOPIC, topic);
    KafkaSource source = new KafkaSource();
    source.doConfigure(context);

    // Produce some data and save an offset
    Long fifthOffset = 0L;
    Long tenthOffset = 0L;
    Properties props = createProducerProps(kafkaServer.getBootstrapServers());
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
    for (int i = 1; i <= 50; i++) {
      ProducerRecord<String, byte[]> data =
          new ProducerRecord<>(topic, null, String.valueOf(i).getBytes());
      RecordMetadata recordMetadata = producer.send(data).get();
      if (i == 5) {
        fifthOffset = recordMetadata.offset();
      }
      if (i == 10) {
        tenthOffset = recordMetadata.offset();
      }
    }

    // Commit 10th offset to zookeeper
    if (hasZookeeperOffsets) {
      ZkUtils zkUtils = ZkUtils.apply(kafkaServer.getZkConnectString(), 30000, 30000,
          JaasUtils.isZkSecurityEnabled());
      ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
      // we commit the tenth offset to ensure some data is missed.
      Long offset = tenthOffset + 1;
      zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir() + "/0", offset.toString(),
          zkUtils.updatePersistentPath$default$3());
      zkUtils.close();
    }

    // Commit 5th offset to kafka
    if (hasKafkaOffsets) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(fifthOffset + 1));
      KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(source.getConsumerProps());
      consumer.commitSync(offsets);
      consumer.close();
    }

    // Start the source and read some data
    source.setChannelProcessor(createGoodChannel());
    source.start();
    Thread.sleep(500L);
    source.process();
    List<Integer> finals = new ArrayList<Integer>(40);
    for (Event event: events) {
      finals.add(Integer.parseInt(new String(event.getBody())));
    }
    source.stop();

    if (!hasKafkaOffsets && !hasZookeeperOffsets) {
      // The default behavior is to start at the latest message in the log
      org.junit.Assert.assertTrue("Source should read no messages", finals.isEmpty());
    } else if (hasKafkaOffsets && hasZookeeperOffsets) {
      // Respect Kafka offsets if they exist
      org.junit.Assert.assertFalse("Source should not read the 5th message", finals.contains(5));
      org.junit.Assert.assertTrue("Source should read the 6th message", finals.contains(6));
    } else if (hasKafkaOffsets) {
      // Respect Kafka offsets if they exist (don't fail if zookeeper offsets are missing)
      org.junit.Assert.assertFalse("Source should not read the 5th message", finals.contains(5));
      org.junit.Assert.assertTrue("Source should read the 6th message", finals.contains(6));
    } else {
      // Otherwise migrate the ZooKeeper offsets if they exist
      org.junit.Assert.assertFalse("Source should not read the 10th message", finals.contains(10));
      org.junit.Assert.assertTrue("Source should read the 11th message", finals.contains(11));
    }
  }

  ChannelProcessor createGoodChannel() {

    ChannelProcessor channelProcessor = mock(ChannelProcessor.class);

    events = Lists.newArrayList();

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        events.addAll((List<Event>)invocation.getArguments()[0]);
        return null;
      }
    }).when(channelProcessor).processEventBatch(any(List.class));

    return channelProcessor;
  }

  ChannelProcessor createBadChannel() {
    ChannelProcessor channelProcessor = mock(ChannelProcessor.class);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new ChannelException("channel intentional broken");
      }
    }).when(channelProcessor).processEventBatch(any(List.class));

    return channelProcessor;
  }

  public String findUnusedTopic() {
    String newTopic = null;
    boolean topicFound = false;
    while (!topicFound) {
      newTopic = RandomStringUtils.randomAlphabetic(8);
      if (!usedTopics.contains(newTopic)) {
        usedTopics.add(newTopic);
        topicFound = true;
      }
    }
    return newTopic;
  }

  private Properties createProducerProps(String bootStrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    return props;
  }
}
