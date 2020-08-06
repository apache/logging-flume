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
package org.apache.flume.channel.kafka;

import kafka.zk.KafkaZkClient;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.GROUP_ID_FLUME;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY;

public class TestOffsetsAndMigration extends TestKafkaChannelBase {

  @Test
  public void testOffsetsNotCommittedOnStop() throws Exception {
    String message = "testOffsetsNotCommittedOnStop-" + System.nanoTime();

    KafkaChannel channel = startChannel(false);

    KafkaProducer<String, byte[]> producer =
        new KafkaProducer<>(channel.getProducerProps());
    ProducerRecord<String, byte[]> data =
        new ProducerRecord<>(topic, "header-" + message, message.getBytes());
    producer.send(data).get();
    producer.flush();
    producer.close();

    Event event = takeEventWithoutCommittingTxn(channel);
    Assert.assertNotNull(event);
    Assert.assertTrue(Arrays.equals(message.getBytes(), event.getBody()));

    // Stop the channel without committing the transaction
    channel.stop();

    channel = startChannel(false);

    // Message should still be available
    event = takeEventWithoutCommittingTxn(channel);
    Assert.assertNotNull(event);
    Assert.assertTrue(Arrays.equals(message.getBytes(), event.getBody()));
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

  private Event takeEventWithoutCommittingTxn(KafkaChannel channel) {
    for (int i = 0; i < 10; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();

      Event event = channel.take();
      if (event != null) {
        return event;
      } else {
        txn.commit();
        txn.close();
      }
    }
    return null;
  }

  private void doTestMigrateZookeeperOffsets(boolean hasZookeeperOffsets, boolean hasKafkaOffsets,
                                             String group) throws Exception {
    // create a topic with 1 partition for simplicity
    topic = findUnusedTopic();
    createTopic(topic, 1);

    Context context = prepareDefaultContext(false);
    context.put(ZOOKEEPER_CONNECT_FLUME_KEY, testUtil.getZkUrl());
    context.put(GROUP_ID_FLUME, group);
    final KafkaChannel channel = createChannel(context);

    // Produce some data and save an offset
    Long fifthOffset = 0L;
    Long tenthOffset = 0L;
    Properties props = channel.getProducerProps();
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
      KafkaZkClient zkClient = KafkaZkClient.apply(testUtil.getZkUrl(),
              JaasUtils.isZkSecurityEnabled(), 30000, 30000, 10, Time.SYSTEM,
              "kafka.server", "SessionExpireListener", scala.Option.empty());
      zkClient.getConsumerOffset(group, new TopicPartition(topic, 0));
      Long offset = tenthOffset + 1;
      zkClient.setOrCreateConsumerOffset(group, new TopicPartition(topic, 0), offset);
      zkClient.close();
    }

    // Commit 5th offset to kafka
    if (hasKafkaOffsets) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      offsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(fifthOffset + 1));
      KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(channel.getConsumerProps());
      consumer.commitSync(offsets);
      consumer.close();
    }

    // Start the channel and read some data
    channel.start();
    ExecutorCompletionService<Void> submitterSvc = new
        ExecutorCompletionService<>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc,
        20, false, false);
    wait(submitterSvc, 5);
    List<Integer> finals = new ArrayList<>(40);
    for (Event event : events) {
      finals.add(Integer.parseInt(new String(event.getBody())));
    }
    channel.stop();

    if (!hasKafkaOffsets && !hasZookeeperOffsets) {
      // The default behavior is to read the entire log
      Assert.assertTrue("Channel should read the the first message", finals.contains(1));
    } else if (hasKafkaOffsets && hasZookeeperOffsets) {
      // Respect Kafka offsets if they exist
      Assert.assertFalse("Channel should not read the 5th message", finals.contains(5));
      Assert.assertTrue("Channel should read the 6th message", finals.contains(6));
    } else if (hasKafkaOffsets) {
      // Respect Kafka offsets if they exist (don't fail if zookeeper offsets are missing)
      Assert.assertFalse("Channel should not read the 5th message", finals.contains(5));
      Assert.assertTrue("Channel should read the 6th message", finals.contains(6));
    } else {
      // Otherwise migrate the ZooKeeper offsets if they exist
      Assert.assertFalse("Channel should not read the 10th message", finals.contains(10));
      Assert.assertTrue("Channel should read the 11th message", finals.contains(11));
    }
  }

  @Test
  public void testMigrateZookeeperOffsetsWhenTopicNotExists() throws Exception {
    topic = findUnusedTopic();

    Context context = prepareDefaultContext(false);
    context.put(ZOOKEEPER_CONNECT_FLUME_KEY, testUtil.getZkUrl());
    context.put(GROUP_ID_FLUME, "testMigrateOffsets-nonExistingTopic");
    KafkaChannel channel = createChannel(context);

    channel.start();

    Assert.assertEquals(LifecycleState.START, channel.getLifecycleState());

    channel.stop();
  }
}
