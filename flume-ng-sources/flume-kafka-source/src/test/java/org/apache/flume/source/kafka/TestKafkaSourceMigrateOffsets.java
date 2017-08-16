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

import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC;
import static org.apache.flume.source.kafka.KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME_KEY;

public class TestKafkaSourceMigrateOffsets extends TestKafkaSourceBase {

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

  private void doTestMigrateZookeeperOffsets(boolean hasZookeeperOffsets, boolean hasKafkaOffsets,
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
      ProducerRecord<String, byte[]> record = new ProducerRecord<>(
          topic, null, String.valueOf(i).getBytes()
      );
      RecordMetadata recordMetadata = producer.send(record).get();
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
}
