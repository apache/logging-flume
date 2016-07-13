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

import com.google.common.collect.Lists;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.GROUP_ID_FLUME;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.KEY_HEADER;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.PARSE_AS_FLUME_EVENT;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.READ_SMALLEST_OFFSET;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.TOPIC_CONFIG;

public class TestKafkaChannel {

  private static TestUtil testUtil = TestUtil.getInstance();
  private String topic = null;
  private final Set<String> usedTopics = new HashSet<String>();

  @BeforeClass
  public static void setupClass() throws Exception {
    testUtil.prepare();
    Thread.sleep(2500);
  }

  @Before
  public void setup() throws Exception {
    boolean topicFound = false;
    while (!topicFound) {
      topic = RandomStringUtils.randomAlphabetic(8);
      if (!usedTopics.contains(topic)) {
        usedTopics.add(topic);
        topicFound = true;
      }
    }
    try {
      createTopic(topic);
    } catch (Exception e) {
    }
    Thread.sleep(2500);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  //Make sure the props are picked up correctly.
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
    context.put(BROKER_LIST_FLUME_KEY,testUtil.getKafkaServerUrl());
    context.put(GROUP_ID_FLUME,"flume-something");
    context.put(READ_SMALLEST_OFFSET,"true");
    context.put("topic",topic);

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
  public void testSuccess() throws Exception {
    doTestSuccessRollback(false, false);
  }

  @Test
  public void testSuccessInterleave() throws Exception {
    doTestSuccessRollback(false, true);
  }

  @Test
  public void testRollbacks() throws Exception {
    doTestSuccessRollback(true, false);
  }

  @Test
  public void testRollbacksInterleave() throws Exception {
    doTestSuccessRollback(true, true);
  }

  private void doTestSuccessRollback(final boolean rollback,
                                     final boolean interleave) throws Exception {
    final KafkaChannel channel = startChannel(true);
    writeAndVerify(rollback, channel, interleave);
    channel.stop();
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
  public void testParseAsFlumeEventFalse() throws Exception {
    doParseAsFlumeEventFalse(false);
  }

  @Test
  public void testParseAsFlumeEventFalseCheckHeader() throws Exception {
    doParseAsFlumeEventFalse(true);
  }

  @Test
  public void testParseAsFlumeEventFalseAsSource() throws Exception {
    doParseAsFlumeEventFalseAsSource(false);
  }

  @Test
  public void testParseAsFlumeEventFalseAsSourceCheckHeader() throws Exception {
    doParseAsFlumeEventFalseAsSource(true);
  }

  @Test
  public void testNullKeyNoHeader() throws Exception {
    doTestNullKeyNoHeader();
  }

  @Test
  public void testOffsetsNotCommittedOnStop() throws Exception {
    String message = "testOffsetsNotCommittedOnStop-" + System.nanoTime();

    KafkaChannel channel = startChannel(false);

    KafkaProducer<String, byte[]> producer =
        new KafkaProducer<String, byte[]>(channel.getProducerProps());
    ProducerRecord<String, byte[]> data =
        new ProducerRecord<String, byte[]>(topic, "header-" + message, message.getBytes());
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

  private Event takeEventWithoutCommittingTxn(KafkaChannel channel) {
    for (int i = 0; i < 5; i++) {
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

  private void doParseAsFlumeEventFalse(Boolean checkHeaders) throws Exception {
    final KafkaChannel channel = startChannel(false);
    Properties props = channel.getProducerProps();
    KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

    for (int i = 0; i < 50; i++) {
      ProducerRecord<String, byte[]> data =
          new ProducerRecord<String, byte[]>(topic, String.valueOf(i) + "-header",
                                             String.valueOf(i).getBytes());
      producer.send(data).get();
    }
    ExecutorCompletionService<Void> submitterSvc = new
        ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc, 50, false, false);
    wait(submitterSvc, 5);
    Map<Integer, String> finals = new HashMap<Integer, String>();
    for (int i = 0; i < 50; i++) {
      finals.put(Integer.parseInt(new String(events.get(i).getBody())),
                 events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.keySet().contains(i));
      if (checkHeaders) {
        Assert.assertTrue(finals.containsValue(String.valueOf(i) + "-header"));
      }
      finals.remove(i);
    }
    Assert.assertTrue(finals.isEmpty());
    channel.stop();
  }

  private void doTestNullKeyNoHeader() throws Exception {
    final KafkaChannel channel = startChannel(false);
    Properties props = channel.getProducerProps();
    KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

    for (int i = 0; i < 50; i++) {
      ProducerRecord<String, byte[]> data =
          new ProducerRecord<String, byte[]>(topic, null, String.valueOf(i).getBytes());
      producer.send(data).get();
    }
    ExecutorCompletionService<Void> submitterSvc = new
            ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc,
            50, false, false);
    wait(submitterSvc, 5);
    List<String> finals = new ArrayList<String>(50);
    for (int i = 0; i < 50; i++) {
      finals.add(i, events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue( finals.get(i) == null);
    }
    channel.stop();
  }

  /**
   * Like the previous test but here we write to the channel like a Flume source would do
   * to verify that the events are written as text and not as an Avro object
   *
   * @throws Exception
   */
  public void doParseAsFlumeEventFalseAsSource(Boolean checkHeaders) throws Exception {
    final KafkaChannel channel = startChannel(false);

    List<String> msgs = new ArrayList<String>();
    Map<String, String> headers = new HashMap<String, String>();
    for (int i = 0; i < 50; i++) {
      msgs.add(String.valueOf(i));
    }
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (int i = 0; i < msgs.size(); i++) {
      headers.put(KEY_HEADER, String.valueOf(i) + "-header");
      channel.put(EventBuilder.withBody(msgs.get(i).getBytes(), headers));
    }
    tx.commit();
    ExecutorCompletionService<Void> submitterSvc =
        new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc, 50, false, false);
    wait(submitterSvc, 5);
    Map<Integer, String> finals = new HashMap<Integer, String>();
    for (int i = 0; i < 50; i++) {
      finals.put(Integer.parseInt(new String(events.get(i).getBody())),
                 events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.keySet().contains(i));
      if (checkHeaders) {
        Assert.assertTrue(finals.containsValue(String.valueOf(i) + "-header"));
      }
      finals.remove(i);
    }
    Assert.assertTrue(finals.isEmpty());
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
            new ExecutorCompletionService<Void>(underlying);
    final List<List<Event>> events = createBaseList();
    putEvents(channel, events, submitterSvc);
    int completed = 0;
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

  private KafkaChannel startChannel(boolean parseAsFlume) throws Exception {
    Context context = prepareDefaultContext(parseAsFlume);
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    channel.start();
    return channel;
  }

  private void writeAndVerify(final boolean testRollbacks,
                              final KafkaChannel channel) throws Exception {
    writeAndVerify(testRollbacks, channel, false);
  }

  private void writeAndVerify(final boolean testRollbacks,
                              final KafkaChannel channel, final boolean interleave)
      throws Exception {

    final List<List<Event>> events = createBaseList();

    ExecutorCompletionService<Void> submitterSvc =
        new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());

    putEvents(channel, events, submitterSvc);

    if (interleave) {
      wait(submitterSvc, 5);
    }

    ExecutorCompletionService<Void> submitterSvc2 =
        new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());

    final List<Event> eventsPulled = pullEvents(channel, submitterSvc2, 50, testRollbacks, true);

    if (!interleave) {
      wait(submitterSvc, 5);
    }
    wait(submitterSvc2, 5);

    verify(eventsPulled);
  }

  private List<List<Event>> createBaseList() {
    final List<List<Event>> events = new ArrayList<List<Event>>();
    for (int i = 0; i < 5; i++) {
      List<Event> eventList = new ArrayList<Event>(10);
      events.add(eventList);
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = new HashMap<String, String>();
        String v = (String.valueOf(i) + " - " + String
                .valueOf(j));
        hdrs.put("header", v);
        eventList.add(EventBuilder.withBody(v.getBytes(), hdrs));
      }
    }
    return events;
  }

  private void putEvents(final KafkaChannel channel, final List<List<Event>>
          events, ExecutorCompletionService<Void> submitterSvc) {
    for (int i = 0; i < 5; i++) {
      final int index = i;
      submitterSvc.submit(new Callable<Void>() {
        @Override
        public Void call() {
          Transaction tx = channel.getTransaction();
          tx.begin();
          List<Event> eventsToPut = events.get(index);
          for (int j = 0; j < 10; j++) {
            channel.put(eventsToPut.get(j));
          }
          try {
            tx.commit();
          } finally {
            tx.close();
          }
          return null;
        }
      });
    }
  }

  private List<Event> pullEvents(final KafkaChannel channel,
                                 ExecutorCompletionService<Void> submitterSvc, final int total,
                                 final boolean testRollbacks, final boolean retryAfterRollback) {
    final List<Event> eventsPulled = Collections.synchronizedList(new
            ArrayList<Event>(50));
    final CyclicBarrier barrier = new CyclicBarrier(5);
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicInteger rolledBackCount = new AtomicInteger(0);
    final AtomicBoolean startedGettingEvents = new AtomicBoolean(false);
    final AtomicBoolean rolledBack = new AtomicBoolean(false);
    for (int k = 0; k < 5; k++) {
      final int index = k;
      submitterSvc.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Transaction tx = null;
          final List<Event> eventsLocal = Lists.newLinkedList();
          int takenByThisThread = 0;
          channel.registerThread();
          Thread.sleep(1000);
          barrier.await();
          while (counter.get() < (total - rolledBackCount.get())) {
            if (tx == null) {
              tx = channel.getTransaction();
              tx.begin();
            }
            try {
              Event e = channel.take();
              if (e != null) {
                startedGettingEvents.set(true);
                eventsLocal.add(e);
              } else {
                if (testRollbacks &&
                        index == 4 &&
                        (!rolledBack.get()) &&
                        startedGettingEvents.get()) {
                  tx.rollback();
                  tx.close();
                  tx = null;
                  rolledBack.set(true);
                  final int eventsLocalSize = eventsLocal.size();
                  eventsLocal.clear();
                  if (!retryAfterRollback) {
                    rolledBackCount.set(eventsLocalSize);
                    return null;
                  }
                } else {
                  tx.commit();
                  tx.close();
                  tx = null;
                  eventsPulled.addAll(eventsLocal);
                  counter.getAndAdd(eventsLocal.size());
                  eventsLocal.clear();
                }
              }
            } catch (Exception ex) {
              eventsLocal.clear();
              if (tx != null) {
                tx.rollback();
                tx.close();
              }
              tx = null;
              ex.printStackTrace();
            }
          }
          // Close txn.
          return null;
        }
      });
    }
    return eventsPulled;
  }

  private void wait(ExecutorCompletionService<Void> submitterSvc, int max)
          throws Exception {
    int completed = 0;
    while (completed < max) {
      submitterSvc.take();
      completed++;
    }
  }

  private void verify(List<Event> eventsPulled) {
    Assert.assertFalse(eventsPulled.isEmpty());
    Assert.assertEquals(50, eventsPulled.size());
    Set<String> eventStrings = new HashSet<String>();
    for (Event e : eventsPulled) {
      Assert.assertEquals(e.getHeaders().get("header"), new String(e.getBody()));
      eventStrings.add(e.getHeaders().get("header"));
    }
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 10; j++) {
        String v = String.valueOf(i) + " - " + String.valueOf(j);
        Assert.assertTrue(eventStrings.contains(v));
        eventStrings.remove(v);
      }
    }
    Assert.assertTrue(eventStrings.isEmpty());
  }

  private Context prepareDefaultContext(boolean parseAsFlume) {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS_CONFIG, testUtil.getKafkaServerUrl());
    context.put(PARSE_AS_FLUME_EVENT, String.valueOf(parseAsFlume));
    context.put(TOPIC_CONFIG, topic);

    return context;
  }

  public static void createTopic(String topicName) {
    int numPartitions = 5;
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkUtils zkUtils =
        ZkUtils.apply(testUtil.getZkUrl(), sessionTimeoutMs, connectionTimeoutMs, false);
    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor, topicConfig);
  }

  public static void deleteTopic(String topicName) {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkUtils zkUtils =
        ZkUtils.apply(testUtil.getZkUrl(), sessionTimeoutMs, connectionTimeoutMs, false);
    AdminUtils.deleteTopic(zkUtils, topicName);
  }
}
