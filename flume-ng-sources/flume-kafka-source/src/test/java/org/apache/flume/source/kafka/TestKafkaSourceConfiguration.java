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

import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flume.source.kafka.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.DEFAULT_AUTO_COMMIT;
import static org.apache.flume.source.kafka.KafkaSourceConstants.KAFKA_CONSUMER_PREFIX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.OLD_GROUP_ID;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS_REGEX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKafkaSourceConfiguration extends TestKafkaSourceBase {

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

  /**
   * Tests that sub-properties (kafka.consumer.*) apply correctly across multiple invocations
   * of configure() (fix for FLUME-2857).
   * @throws Exception
   */
  @Test
  public void testDefaultSettingsOnReConfigure() throws Exception {
    String sampleConsumerProp = "auto.offset.reset";
    String sampleConsumerVal = "earliest";
    String group = "group";

    Context context = prepareDefaultContext(group);
    context.put(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX + sampleConsumerProp,
        sampleConsumerVal);
    context.put(TOPIC, "random-topic");

    kafkaSource.configure(context);

    Assert.assertEquals(sampleConsumerVal,
        kafkaSource.getConsumerProps().getProperty(sampleConsumerProp));

    context = prepareDefaultContext(group);
    context.put(TOPIC, "random-topic");

    kafkaSource.configure(context);
    Assert.assertNull(kafkaSource.getConsumerProps().getProperty(sampleConsumerProp));
  }
}
