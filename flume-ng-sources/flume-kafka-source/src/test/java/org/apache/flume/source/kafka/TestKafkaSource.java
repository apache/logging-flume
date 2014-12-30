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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import kafka.common.TopicExistsException;
import kafka.consumer.ConsumerIterator;
import kafka.message.Message;

import org.apache.flume.*;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKafkaSource {
  private static final Logger log =
          LoggerFactory.getLogger(TestKafkaSource.class);

  private KafkaSource kafkaSource;
  private KafkaSourceEmbeddedKafka kafkaServer;
  private ConsumerIterator<byte[], byte[]> mockIt;
  private Message message;
  private Context context;
  private List<Event> events;
  private String topicName = "test1";


  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {

    kafkaSource = new KafkaSource();
    kafkaServer = new KafkaSourceEmbeddedKafka();
    try {
      kafkaServer.createTopic(topicName);
    } catch (TopicExistsException e) {
      //do nothing
    }


    context = new Context();
    context.put(KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME,
            kafkaServer.getZkConnectString());
    context.put(KafkaSourceConstants.GROUP_ID_FLUME,"flume");
    context.put(KafkaSourceConstants.TOPIC,topicName);
    context.put("kafka.consumer.timeout.ms","100");

    kafkaSource.setChannelProcessor(createGoodChannel());
  }

  @After
  public void tearDown() throws Exception {
    kafkaSource.stop();
    kafkaServer.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmpty() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "hello, world");

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
    context.put(KafkaSourceConstants.BATCH_SIZE,"2");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "hello, world");
    kafkaServer.produce(topicName, "", "foo, bar");

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
    context.put(KafkaSourceConstants.TOPIC,"faketopic");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
  }

  @SuppressWarnings("unchecked")
  @Test(expected= FlumeException.class)
  public void testNonExistingZk() throws EventDeliveryException,
          SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException, InterruptedException {
    context.put(KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME,"blabla:666");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    Status status = kafkaSource.process();
    assertEquals(Status.BACKOFF, status);
  }

  @Test
  public void testBatchTime() throws InterruptedException,
          EventDeliveryException {
    context.put(KafkaSourceConstants.BATCH_DURATION_MS,"250");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    for (int i=1; i<5000; i++) {
      kafkaServer.produce(topicName, "", "hello, world " + i);
    }
    Thread.sleep(500L);

    long startTime = System.currentTimeMillis();
    Status status = kafkaSource.process();
    long endTime = System.currentTimeMillis();
    assertEquals(Status.READY, status);
    assertTrue(endTime - startTime <
            ( context.getLong(KafkaSourceConstants.BATCH_DURATION_MS) +
            context.getLong("kafka.consumer.timeout.ms")) );
  }

  // Consume event, stop source, start again and make sure we are not
  // consuming same event again
  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {
    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "hello, world");

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
  public void testNonCommit() throws EventDeliveryException,
          InterruptedException {

    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    context.put(KafkaSourceConstants.BATCH_DURATION_MS,"30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "hello, world");
    Thread.sleep(500L);

    kafkaSource.setChannelProcessor(createBadChannel());
    log.debug("processing from kafka to bad channel");
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());

    log.debug("repairing channel");
    kafkaSource.setChannelProcessor(createGoodChannel());

    log.debug("re-process to good channel - this should work");
    kafkaSource.process();
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
            Charsets.UTF_8));


  }

  @Test
  public void testTwoBatches() throws InterruptedException,
          EventDeliveryException {
    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    context.put(KafkaSourceConstants.BATCH_DURATION_MS,"30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(),
            Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topicName, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(),
            Charsets.UTF_8));
  }

  @Test
  public void testTwoBatchesWithAutocommit() throws InterruptedException,
          EventDeliveryException {
    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    context.put(KafkaSourceConstants.BATCH_DURATION_MS,"30000");
    context.put("kafka.auto.commit.enable","true");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topicName, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(),
            Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topicName, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(),
            Charsets.UTF_8));

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullKey() throws EventDeliveryException,
      SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(KafkaSourceConstants.BATCH_SIZE,"1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topicName, null , "hello, world");

    Thread.sleep(500L);

    Assert.assertEquals(Status.READY, kafkaSource.process());
    Assert.assertEquals(Status.BACKOFF, kafkaSource.process());
    Assert.assertEquals(1, events.size());

    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
        Charsets.UTF_8));
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
}