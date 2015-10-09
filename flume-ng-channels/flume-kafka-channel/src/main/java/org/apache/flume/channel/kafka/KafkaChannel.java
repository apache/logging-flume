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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.*;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.*;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaChannel extends BasicChannelSemantics {

  private final static Logger LOGGER =
    LoggerFactory.getLogger(KafkaChannel.class);


  private final Properties kafkaConf = new Properties();
  private Producer<String, byte[]> producer;
  private final String channelUUID = UUID.randomUUID().toString();

  private AtomicReference<String> topic = new AtomicReference<String>();
  private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
  private final Map<String, Integer> topicCountMap =
    Collections.synchronizedMap(new HashMap<String, Integer>());

  // Track all consumers to close them eventually.
  private final List<ConsumerAndIterator> consumers =
    Collections.synchronizedList(new LinkedList<ConsumerAndIterator>());

  private KafkaChannelCounter counter;

  /* Each ConsumerConnector commit will commit all partitions owned by it. To
   * ensure that each partition is only committed when all events are
   * actually done, we will need to keep a ConsumerConnector per thread.
   * See Neha's answer here:
   * http://grokbase.com/t/kafka/users/13b4gmk2jk/commit-offset-per-topic
   * Since only one consumer connector will a partition at any point in time,
   * when we commit the partition we would have committed all events to the
   * final destination from that partition.
   *
   * If a new partition gets assigned to this connector,
   * my understanding is that all message from the last partition commit will
   * get replayed which may cause duplicates -- which is fine as this
   * happens only on partition rebalancing which is on failure or new nodes
   * coming up, which is rare.
   */
  private final ThreadLocal<ConsumerAndIterator> consumerAndIter = new
    ThreadLocal<ConsumerAndIterator>() {
      @Override
      public ConsumerAndIterator initialValue() {
        return createConsumerAndIter();
      }
    };

  @Override
  public void start() {
    try {
      LOGGER.info("Starting Kafka Channel: " + getName());
      producer = new Producer<String, byte[]>(new ProducerConfig(kafkaConf));
      // We always have just one topic being read by one thread
      LOGGER.info("Topic = " + topic.get());
      topicCountMap.put(topic.get(), 1);
      counter.start();
      super.start();
    } catch (Exception e) {
      LOGGER.error("Could not start producer");
      throw new FlumeException("Unable to create Kafka Connections. " +
        "Check whether Kafka Brokers are up and that the " +
        "Flume agent can connect to it.", e);
    }
  }

  @Override
  public void stop() {
    for (ConsumerAndIterator c : consumers) {
      try {
        decommissionConsumerAndIterator(c);
      } catch (Exception ex) {
        LOGGER.warn("Error while shutting down consumer.", ex);
      }
    }
    producer.close();
    counter.stop();
    super.stop();
    LOGGER.info("Kafka channel {} stopped. Metrics: {}", getName(),
        counter);
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new KafkaTransaction();
  }

  private synchronized ConsumerAndIterator createConsumerAndIter() {
    try {
      ConsumerConfig consumerConfig = new ConsumerConfig(kafkaConf);
      ConsumerConnector consumer =
        Consumer.createJavaConsumerConnector(consumerConfig);
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        consumer.createMessageStreams(topicCountMap);
      final List<KafkaStream<byte[], byte[]>> streamList = consumerMap
        .get(topic.get());
      KafkaStream<byte[], byte[]> stream = streamList.remove(0);
      ConsumerAndIterator ret =
        new ConsumerAndIterator(consumer, stream.iterator(), channelUUID);
      consumers.add(ret);
      LOGGER.info("Created new consumer to connect to Kafka");
      return ret;
    } catch (Exception e) {
      throw new FlumeException("Unable to connect to Kafka", e);
    }
  }

  Properties getKafkaConf() {
    return kafkaConf;
  }

  @Override
  public void configure(Context ctx) {
    String topicStr = ctx.getString(TOPIC);
    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = DEFAULT_TOPIC;
      LOGGER
        .info("Topic was not specified. Using " + topicStr + " as the topic.");
    }
    topic.set(topicStr);
    String groupId = ctx.getString(GROUP_ID_FLUME);
    if (groupId == null || groupId.isEmpty()) {
      groupId = DEFAULT_GROUP_ID;
      LOGGER.info(
        "Group ID was not specified. Using " + groupId + " as the group id.");
    }
    String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
    if (brokerList == null || brokerList.isEmpty()) {
      throw new ConfigurationException("Broker List must be specified");
    }
    String zkConnect = ctx.getString(ZOOKEEPER_CONNECT_FLUME_KEY);
    if (zkConnect == null || zkConnect.isEmpty()) {
      throw new ConfigurationException(
        "Zookeeper Connection must be specified");
    }
    kafkaConf.putAll(ctx.getSubProperties(KAFKA_PREFIX));
    kafkaConf.put(GROUP_ID, groupId);
    kafkaConf.put(BROKER_LIST_KEY, brokerList);
    kafkaConf.put(ZOOKEEPER_CONNECT, zkConnect);
    kafkaConf.put(AUTO_COMMIT_ENABLED, String.valueOf(false));
    if(kafkaConf.get(CONSUMER_TIMEOUT) == null) {
      kafkaConf.put(CONSUMER_TIMEOUT, DEFAULT_TIMEOUT);
    }
    kafkaConf.put(REQUIRED_ACKS_KEY, "-1");
    LOGGER.info(kafkaConf.toString());
    parseAsFlumeEvent =
      ctx.getBoolean(PARSE_AS_FLUME_EVENT, DEFAULT_PARSE_AS_FLUME_EVENT);

    boolean readSmallest = ctx.getBoolean(READ_SMALLEST_OFFSET,
      DEFAULT_READ_SMALLEST_OFFSET);
    // If the data is to be parsed as Flume events, we always read the smallest.
    // Else, we read the configuration, which by default reads the largest.
    if (parseAsFlumeEvent || readSmallest) {
      // readSmallest is eval-ed only if parseAsFlumeEvent is false.
      // The default is largest, so we don't need to set it explicitly.
      kafkaConf.put("auto.offset.reset", "smallest");
    }

    if (counter == null) {
      counter = new KafkaChannelCounter(getName());
    }

  }

  private void decommissionConsumerAndIterator(ConsumerAndIterator c) {
    if (c.failedEvents.isEmpty()) {
      c.consumer.commitOffsets();
    }
    c.failedEvents.clear();
    c.consumer.shutdown();
  }

  // Force a consumer to be initialized. There are  many duplicates in
  // tests due to rebalancing - making testing tricky. In production,
  // this is less of an issue as
  // rebalancing would happen only on startup.
  @VisibleForTesting
  void registerThread() {
    consumerAndIter.get();
  }

  private enum TransactionType {
    PUT,
    TAKE,
    NONE
  }


  private class KafkaTransaction extends BasicTransactionSemantics {

    private TransactionType type = TransactionType.NONE;
    // For Puts
    private Optional<ByteArrayOutputStream> tempOutStream = Optional
      .absent();

    // For put transactions, serialize the events and batch them and send it.
    private Optional<LinkedList<byte[]>> serializedEvents = Optional.absent();
    // For take transactions, deserialize and hold them till commit goes through
    private Optional<LinkedList<Event>> events = Optional.absent();
    private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer =
      Optional.absent();
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader =
      Optional.absent();

    // Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;
    private BinaryDecoder decoder = null;
    private final String batchUUID = UUID.randomUUID().toString();
    private boolean eventTaken = false;

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;
      if (!serializedEvents.isPresent()) {
        serializedEvents = Optional.of(new LinkedList<byte[]>());
      }

      try {
        if (parseAsFlumeEvent) {
          if (!tempOutStream.isPresent()) {
            tempOutStream = Optional.of(new ByteArrayOutputStream());
          }
          if (!writer.isPresent()) {
            writer = Optional.of(new
              SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
          }
          tempOutStream.get().reset();
          AvroFlumeEvent e = new AvroFlumeEvent(
            toCharSeqMap(event.getHeaders()),
            ByteBuffer.wrap(event.getBody()));
          encoder = EncoderFactory.get()
            .directBinaryEncoder(tempOutStream.get(), encoder);
          writer.get().write(e, encoder);
          // Not really possible to avoid this copy :(
          serializedEvents.get().add(tempOutStream.get().toByteArray());
        } else {
          serializedEvents.get().add(event.getBody());
        }
      } catch (Exception e) {
        throw new ChannelException("Error while serializing event", e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Event doTake() throws InterruptedException {
      type = TransactionType.TAKE;
      try {
        if (!(consumerAndIter.get().uuid.equals(channelUUID))) {
          LOGGER.info("UUID mismatch, creating new consumer");
          decommissionConsumerAndIterator(consumerAndIter.get());
          consumerAndIter.remove();
        }
      } catch (Exception ex) {
        LOGGER.warn("Error while shutting down consumer", ex);
      }
      if (!events.isPresent()) {
        events = Optional.of(new LinkedList<Event>());
      }
      Event e;
      if (!consumerAndIter.get().failedEvents.isEmpty()) {
        e = consumerAndIter.get().failedEvents.removeFirst();
      } else {
        try {
          ConsumerIterator<byte[], byte[]> it = consumerAndIter.get().iterator;
          long startTime = System.nanoTime();
          it.hasNext();
          long endTime = System.nanoTime();
          counter.addToKafkaEventGetTimer((endTime-startTime)/(1000*1000));
          if (parseAsFlumeEvent) {
            ByteArrayInputStream in =
              new ByteArrayInputStream(it.next().message());
            decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
            if (!reader.isPresent()) {
              reader = Optional.of(
                new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
            }
            AvroFlumeEvent event = reader.get().read(null, decoder);
            e = EventBuilder.withBody(event.getBody().array(),
              toStringMap(event.getHeaders()));
          } else {
            e = EventBuilder.withBody(it.next().message(),
              Collections.EMPTY_MAP);
          }

        } catch (ConsumerTimeoutException ex) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Timed out while waiting for data to come from Kafka",
              ex);
          }
          return null;
        } catch (Exception ex) {
          LOGGER.warn("Error while getting events from Kafka", ex);
          throw new ChannelException("Error while getting events from Kafka",
            ex);
        }
      }
      eventTaken = true;
      events.get().add(e);
      return e;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {
        try {
          List<KeyedMessage<String, byte[]>> messages = new
            ArrayList<KeyedMessage<String, byte[]>>(serializedEvents.get()
            .size());
          for (byte[] event : serializedEvents.get()) {
            messages.add(new KeyedMessage<String, byte[]>(topic.get(), null,
              batchUUID, event));
          }
          long startTime = System.nanoTime();
          producer.send(messages);
          long endTime = System.nanoTime();
          counter.addToKafkaEventSendTimer((endTime-startTime)/(1000*1000));
          counter.addToEventPutSuccessCount(Long.valueOf(messages.size()));
          serializedEvents.get().clear();
        } catch (Exception ex) {
          LOGGER.warn("Sending events to Kafka failed", ex);
          throw new ChannelException("Commit failed as send to Kafka failed",
            ex);
        }
      } else {
        if (consumerAndIter.get().failedEvents.isEmpty() && eventTaken) {
          long startTime = System.nanoTime();
          consumerAndIter.get().consumer.commitOffsets();
          long endTime = System.nanoTime();
          counter.addToKafkaCommitTimer((endTime-startTime)/(1000*1000));
         }
        counter.addToEventTakeSuccessCount(Long.valueOf(events.get().size()));
        events.get().clear();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {
        serializedEvents.get().clear();
      } else {
        counter.addToRollbackCounter(Long.valueOf(events.get().size()));
        consumerAndIter.get().failedEvents.addAll(events.get());
        events.get().clear();
      }
    }
  }


  private class ConsumerAndIterator {
    final ConsumerConnector consumer;
    final ConsumerIterator<byte[], byte[]> iterator;
    final String uuid;
    final LinkedList<Event> failedEvents = new LinkedList<Event>();

    ConsumerAndIterator(ConsumerConnector consumerConnector,
      ConsumerIterator<byte[], byte[]> iterator, String uuid) {
      this.consumer = consumerConnector;
      this.iterator = iterator;
      this.uuid = uuid;
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
    Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
      new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(
    Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
      new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }
}
