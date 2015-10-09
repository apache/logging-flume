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
import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.junit.*;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestKafkaChannel {

  private static TestUtil testUtil = TestUtil.getInstance();
  private String topic = null;
  private final Set<String> usedTopics = new HashSet<String>();
  private CountDownLatch latch = null;

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
    latch = new CountDownLatch(5);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
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
  public void testNoParsingAsFlumeAgent() throws Exception {
    final KafkaChannel channel = startChannel(false);
    Producer<String, byte[]> producer = new Producer<String, byte[]>(
      new ProducerConfig(channel.getKafkaConf()));
    List<KeyedMessage<String, byte[]>> original = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      KeyedMessage<String, byte[]> data = new KeyedMessage<String,
        byte[]>(topic, null, RandomStringUtils.randomAlphabetic(6),
        String.valueOf(i).getBytes());
      original.add(data);
    }
    producer.send(original);
    ExecutorCompletionService<Void> submitterSvc = new
      ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc,
      50, false, false);
    wait(submitterSvc, 5);
    Set<Integer> finals = Sets.newHashSet();
    for (int i = 0; i < 50; i++) {
      finals.add(Integer.parseInt(new String(events.get(i).getBody())));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.contains(i));
      finals.remove(i);
    }
    Assert.assertTrue(finals.isEmpty());
    channel.stop();
  }

  @Test
  public void testTimeoutConfig() throws Exception {
    Context context = prepareDefaultContext(true);
    KafkaChannel channel = new KafkaChannel();
    Configurables.configure(channel, context);
    Assert.assertTrue(channel.getKafkaConf().get(CONSUMER_TIMEOUT)
      .equals(DEFAULT_TIMEOUT));

    String timeout = "1000";
    context.put("kafka."+CONSUMER_TIMEOUT, timeout);
    channel = new KafkaChannel();
    Configurables.configure(channel, context);
    Assert.assertTrue(channel.getKafkaConf().get(CONSUMER_TIMEOUT)
            .equals(timeout));
  }

  /**
   * Like the previous test but here we write to the channel like a Flume source would do
   * to verify that the events are written as text and not as an Avro object
   *
   * @throws Exception
   */
  @Test
  public void testWritingToNoParsingAsFlumeAgent() throws Exception {
    final KafkaChannel channel = startChannel(false);

    List<String> msgs = new ArrayList<String>();
    for (int i = 0; i < 50; i++){
      msgs.add(String.valueOf(i));
    }
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (int i = 0; i < msgs.size(); i++){
      channel.put(EventBuilder.withBody(msgs.get(i).getBytes()));
    }
    tx.commit();
    ExecutorCompletionService<Void> submitterSvc = new
            ExecutorCompletionService<Void>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc,
      50, false, false);
    wait(submitterSvc, 5);
    Set<Integer> finals = Sets.newHashSet();
    for (int i = 0; i < 50; i++) {
      finals.add(Integer.parseInt(new String(events.get(i).getBody())));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.contains(i));
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
    final KafkaChannel channel, final boolean interleave) throws Exception {

    final List<List<Event>> events = createBaseList();

    ExecutorCompletionService<Void> submitterSvc =
      new ExecutorCompletionService<Void>(Executors
        .newCachedThreadPool());

    putEvents(channel, events, submitterSvc);

    if (interleave) {
      wait(submitterSvc, 5);
    }

    ExecutorCompletionService<Void> submitterSvc2 =
      new ExecutorCompletionService<Void>(Executors
        .newCachedThreadPool());

    final List<Event> eventsPulled =
      pullEvents(channel, submitterSvc2, 50, testRollbacks, true);

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
      Assert
        .assertEquals(e.getHeaders().get("header"), new String(e.getBody()));
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
    context.put(KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY,
      testUtil.getKafkaServerUrl());
    context.put(KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY,
      testUtil.getZkUrl());
    context.put(KafkaChannelConfiguration.PARSE_AS_FLUME_EVENT,
      String.valueOf(parseAsFlume));
    context.put(KafkaChannelConfiguration.READ_SMALLEST_OFFSET, "true");
    context.put(KafkaChannelConfiguration.TOPIC, topic);
    return context;
  }

  public static void createTopic(String topicName) {
    int numPartitions = 5;
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(testUtil.getZkUrl(),
      sessionTimeoutMs, connectionTimeoutMs,
      ZKStringSerializer$.MODULE$);

    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkClient, topicName, numPartitions,
      replicationFactor, topicConfig);
  }

  public static void deleteTopic(String topicName) {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(testUtil.getZkUrl(),
      sessionTimeoutMs, connectionTimeoutMs,
      ZKStringSerializer$.MODULE$);
    AdminUtils.deleteTopic(zkClient, topicName);
  }
}
