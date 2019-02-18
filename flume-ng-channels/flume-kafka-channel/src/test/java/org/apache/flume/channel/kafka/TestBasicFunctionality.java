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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.GROUP_ID_FLUME;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.KEY_HEADER;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.READ_SMALLEST_OFFSET;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.TOPIC_CONFIG;

public class TestBasicFunctionality extends TestKafkaChannelBase {

  @Test
  public void testProps() throws Exception {
    Context context = new Context();
    context.put("kafka.producer.some-parameter", "1");
    context.put("kafka.consumer.another-parameter", "1");
    context.put(BOOTSTRAP_SERVERS_CONFIG, testUtil.getKafkaServerUrl());
    context.put(TOPIC_CONFIG, topic);

    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);

    Properties consumerProps = channel.getConsumerProps();
    Properties producerProps = channel.getProducerProps();

    Assert.assertEquals(producerProps.getProperty("some-parameter"), "1");
    Assert.assertEquals(consumerProps.getProperty("another-parameter"), "1");
  }

  @Test
  public void testOldConfig() throws Exception {
    Context context = new Context();
    context.put(BROKER_LIST_FLUME_KEY, testUtil.getKafkaServerUrl());
    context.put(GROUP_ID_FLUME, "flume-something");
    context.put(READ_SMALLEST_OFFSET, "true");
    context.put("topic", topic);

    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);

    Properties consumerProps = channel.getConsumerProps();
    Properties producerProps = channel.getProducerProps();

    Assert.assertEquals(producerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
        testUtil.getKafkaServerUrl());
    Assert.assertEquals(consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG),
        "flume-something");
    Assert.assertEquals(consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
        "earliest");
  }


  @Test
  public void testStopAndStart() throws Exception {
    doTestStopAndStart(false, false);
  }

  @Test
  public void testStopAndStartWithRollback() throws Exception {
    doTestStopAndStart(true, true);
  }

  @Test
  public void testStopAndStartWithRollbackAndNoRetry() throws Exception {
    doTestStopAndStart(true, false);
  }

  @Test
  public void testNullKeyNoHeader() throws Exception {
    doTestNullKeyNoHeader();
  }

  /**
   * Tests that sub-properties get set correctly if you run the configure() method twice
   * (fix for FLUME-2857)
   *
   * @throws Exception
   */
  @Test
  public void testDefaultSettingsOnReConfigure() throws Exception {
    String sampleProducerProp = "compression.type";
    String sampleProducerVal = "snappy";

    String sampleConsumerProp = "fetch.min.bytes";
    String sampleConsumerVal = "99";

    Context context = prepareDefaultContext(false);
    context.put(KafkaChannelConfiguration.KAFKA_PRODUCER_PREFIX + sampleProducerProp,
        sampleProducerVal);
    context.put(KafkaChannelConfiguration.KAFKA_CONSUMER_PREFIX + sampleConsumerProp,
        sampleConsumerVal);

    final KafkaChannel channel = createChannel(context);

    Assert.assertEquals(sampleProducerVal,
        channel.getProducerProps().getProperty(sampleProducerProp));
    Assert.assertEquals(sampleConsumerVal,
        channel.getConsumerProps().getProperty(sampleConsumerProp));

    context = prepareDefaultContext(false);
    channel.configure(context);

    Assert.assertNull(channel.getProducerProps().getProperty(sampleProducerProp));
    Assert.assertNull(channel.getConsumerProps().getProperty(sampleConsumerProp));

  }


  private void doTestNullKeyNoHeader() throws Exception {
    final KafkaChannel channel = startChannel(false);
    Properties props = channel.getProducerProps();
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 50; i++) {
      ProducerRecord<String, byte[]> data =
          new ProducerRecord<>(topic, null, String.valueOf(i).getBytes());
      producer.send(data).get();
    }
    ExecutorCompletionService<Void> submitterSvc = new
        ExecutorCompletionService<>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc,
        50, false, false);
    wait(submitterSvc, 5);
    List<String> finals = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      finals.add(i, events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.get(i) == null);
    }
    channel.stop();
  }

  /**
   * This method starts a channel, puts events into it. The channel is then
   * stopped and restarted. Then we check to make sure if all events we put
   * come out. Optionally, 10 events are rolled back,
   * and optionally we restart the agent immediately after and we try to pull it
   * out.
   *
   * @param rollback
   * @param retryAfterRollback
   * @throws Exception
   */
  private void doTestStopAndStart(boolean rollback,
                                  boolean retryAfterRollback) throws Exception {
    final KafkaChannel channel = startChannel(true);
    ExecutorService underlying = Executors
        .newCachedThreadPool();
    ExecutorCompletionService<Void> submitterSvc =
        new ExecutorCompletionService<>(underlying);
    final List<List<Event>> events = createBaseList();
    putEvents(channel, events, submitterSvc);
    wait(submitterSvc, 5);
    channel.stop();
    final KafkaChannel channel2 = startChannel(true);
    int total = 50;
    if (rollback && !retryAfterRollback) {
      total = 40;
    }
    final List<Event> eventsPulled =
        pullEvents(channel2, submitterSvc, total, rollback, retryAfterRollback);
    wait(submitterSvc, 5);
    channel2.stop();
    if (!retryAfterRollback && rollback) {
      final KafkaChannel channel3 = startChannel(true);
      int expectedRemaining = 50 - eventsPulled.size();
      final List<Event> eventsPulled2 =
          pullEvents(channel3, submitterSvc, expectedRemaining, false, false);
      wait(submitterSvc, 5);
      Assert.assertEquals(expectedRemaining, eventsPulled2.size());
      eventsPulled.addAll(eventsPulled2);
      channel3.stop();
    }
    underlying.shutdownNow();
    verify(eventsPulled);
  }

  @Test
  public void testMetricsCount() throws Exception {
    final KafkaChannel channel = startChannel(true);
    ExecutorService underlying = Executors.newCachedThreadPool();
    ExecutorCompletionService<Void> submitterSvc = new ExecutorCompletionService<Void>(underlying);
    final List<List<Event>> events = createBaseList();
    putEvents(channel, events, submitterSvc);
    takeEventsWithCommittingTxn(channel,50);

    KafkaChannelCounter counter =
            (KafkaChannelCounter) Whitebox.getInternalState(channel, "counter");
    Assert.assertEquals(50, counter.getEventPutAttemptCount());
    Assert.assertEquals(50, counter.getEventPutSuccessCount());
    Assert.assertEquals(50, counter.getEventTakeAttemptCount());
    Assert.assertEquals(50, counter.getEventTakeSuccessCount());
    channel.stop();
  }

  private void takeEventsWithCommittingTxn(KafkaChannel channel, long eventsCount) {
    List<Event> takeEventsList = new ArrayList<>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    while (takeEventsList.size() < eventsCount) {
      Event event = channel.take();
      if (event != null) {
        takeEventsList.add(event);
      }
    }
    txn.commit();
    txn.close();
  }
}
