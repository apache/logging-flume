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

import com.google.common.collect.Lists;
import kafka.common.TopicExistsException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flume.source.kafka.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.KAFKA_CONSUMER_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public abstract class TestKafkaSourceBase {
  static final Logger log = LoggerFactory.getLogger(TestKafkaSourceBase.class);

  KafkaSource kafkaSource;
  KafkaSourceEmbeddedKafka kafkaServer;
  Context context;
  List<Event> events;

  private final Set<String> usedTopics = new HashSet<String>();
  String topic0 = "test1";
  String topic1 = "topic1";

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

  Context prepareDefaultContext(String groupId) {
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

  Properties createProducerProps(String bootStrapServers) {
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
