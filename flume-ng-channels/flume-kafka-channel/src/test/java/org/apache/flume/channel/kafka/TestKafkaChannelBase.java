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
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.KAFKA_CONSUMER_PREFIX;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.PARSE_AS_FLUME_EVENT;
import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.TOPIC_CONFIG;

public class TestKafkaChannelBase {

  static TestUtil testUtil = TestUtil.getInstance();
  String topic = null;
  private final Set<String> usedTopics = new HashSet<>();

  static final int DEFAULT_TOPIC_PARTITIONS = 5;

  @BeforeClass
  public static void setupClass() throws Exception {
    testUtil.prepare();
    Thread.sleep(2500);
  }

  @Before
  public void setup() throws Exception {
    topic = findUnusedTopic();
    createTopic(topic, DEFAULT_TOPIC_PARTITIONS);
    Thread.sleep(2500);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  String findUnusedTopic() {
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

  static void createTopic(String topicName, int numPartitions) {
    testUtil.createTopics(Collections.singletonList(topicName), numPartitions);
  }

  static void deleteTopic(String topicName) {
    testUtil.deleteTopic(topicName);
  }

  KafkaChannel startChannel(boolean parseAsFlume) throws Exception {
    Context context = prepareDefaultContext(parseAsFlume);
    KafkaChannel channel = createChannel(context);
    channel.start();
    return channel;
  }

  Context prepareDefaultContext(boolean parseAsFlume) {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS_CONFIG, testUtil.getKafkaServerUrl());
    context.put(PARSE_AS_FLUME_EVENT, String.valueOf(parseAsFlume));
    context.put(TOPIC_CONFIG, topic);
    context.put(KAFKA_CONSUMER_PREFIX + "max.poll.interval.ms", "10000");

    return context;
  }

  KafkaChannel createChannel(Context context) throws Exception {
    final KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    return channel;
  }

  List<Event> pullEvents(final KafkaChannel channel,
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

  void wait(ExecutorCompletionService<Void> submitterSvc, int max)
      throws Exception {
    int completed = 0;
    while (completed < max) {
      submitterSvc.take();
      completed++;
    }
  }

  List<List<Event>> createBaseList() {
    final List<List<Event>> events = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      List<Event> eventList = new ArrayList<>(10);
      events.add(eventList);
      for (int j = 0; j < 10; j++) {
        Map<String, String> hdrs = new HashMap<>();
        String v = (String.valueOf(i) + " - " + String
            .valueOf(j));
        hdrs.put("header", v);
        eventList.add(EventBuilder.withBody(v.getBytes(), hdrs));
      }
    }
    return events;
  }

  void putEvents(final KafkaChannel channel, final List<List<Event>>
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

  void verify(List<Event> eventsPulled) {
    Assert.assertFalse(eventsPulled.isEmpty());
    Assert.assertEquals(50, eventsPulled.size());
    Set<String> eventStrings = new HashSet<>();
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
}
