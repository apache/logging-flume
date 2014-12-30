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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import kafka.message.MessageAndMetadata;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Source for Kafka which reads messages from a kafka topic.
 *
 * <tt>zookeeperConnect: </tt> Kafka's zookeeper connection string.
 * <b>Required</b>
 * <p>
 * <tt>groupId: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>topic: </tt> the topic to consume messages from. <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one
 * batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a
 * batch (of any size) will be written to a channel. Default: 1000
 * <p>
 * <tt>kafka.auto.commit.enable: </tt> If true, commit automatically every time
 * period. if false, commit on each batch. Default: false
 * <p>
 * <tt>kafka.consumer.timeout.ms: </tt> Polling interval for new data for batch.
 * Low value means more CPU usage. High value means the time.upper.limit may be
 * missed. Default: 10
 *
 * Any property starting with "kafka" will be passed to the kafka consumer So
 * you can use any configuration supported by Kafka 0.8.1.1
 */
public class KafkaSource extends AbstractSource
        implements Configurable, PollableSource {
  private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
  private ConsumerConnector consumer;
  private ConsumerIterator<byte[],byte[]> it;
  private String topic;
  private int batchUpperLimit;
  private int timeUpperLimit;
  private int consumerTimeout;
  private boolean kafkaAutoCommitEnabled;
  private Context context;
  private Properties kafkaProps;
  private final List<Event> eventList = new ArrayList<Event>();

  public Status process() throws EventDeliveryException {

    byte[] kafkaMessage;
    byte[] kafkaKey;
    Event event;
    Map<String, String> headers;
    long batchStartTime = System.currentTimeMillis();
    long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
    try {
      boolean iterStatus = false;
      while (eventList.size() < batchUpperLimit &&
              System.currentTimeMillis() < batchEndTime) {
        iterStatus = hasNext();
        if (iterStatus) {
          // get next message
          MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
          kafkaMessage = messageAndMetadata.message();
          kafkaKey = messageAndMetadata.key();

          // Add headers to event (topic, timestamp, and key)
          headers = new HashMap<String, String>();
          headers.put(KafkaSourceConstants.TIMESTAMP,
                  String.valueOf(System.currentTimeMillis()));
          headers.put(KafkaSourceConstants.TOPIC, topic);
          if (kafkaKey != null) {
            headers.put(KafkaSourceConstants.KEY, new String(kafkaKey));
          }
          if (log.isDebugEnabled()) {
            log.debug("Message: {}", new String(kafkaMessage));
          }
          event = EventBuilder.withBody(kafkaMessage, headers);
          eventList.add(event);
        }
        if (log.isDebugEnabled()) {
          log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
          log.debug("Event #: {}", eventList.size());
        }
      }
      // If we have events, send events to channel
      // clear the event list
      // and commit if Kafka doesn't auto-commit
      if (eventList.size() > 0) {
        getChannelProcessor().processEventBatch(eventList);
        eventList.clear();
        if (log.isDebugEnabled()) {
          log.debug("Wrote {} events to channel", eventList.size());
        }
        if (!kafkaAutoCommitEnabled) {
          // commit the read transactions to Kafka to avoid duplicates
          consumer.commitOffsets();
        }
      }
      if (!iterStatus) {
        if (log.isDebugEnabled()) {
          log.debug("Returning with backoff. No more data to read");
        }
        return Status.BACKOFF;
      }
      return Status.READY;
    } catch (Exception e) {
      log.error("KafkaSource EXCEPTION, {}", e);
      return Status.BACKOFF;
    }
  }

  /**
   * We configure the source and generate properties for the Kafka Consumer
   *
   * Kafka Consumer properties are generated as follows:
   *
   * 1. Generate a properties object with some static defaults that can be
   * overridden by Source configuration 2. We add the configuration users added
   * for Kafka (parameters starting with kafka. and must be valid Kafka Consumer
   * properties 3. We add the source documented parameters which can override
   * other properties
   *
   * @param context
   */
  public void configure(Context context) {
    this.context = context;
    batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
            KafkaSourceConstants.DEFAULT_BATCH_SIZE);
    timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
            KafkaSourceConstants.DEFAULT_BATCH_DURATION);
    topic = context.getString(KafkaSourceConstants.TOPIC);

    if(topic == null) {
      throw new ConfigurationException("Kafka topic must be specified.");
    }

    kafkaProps = KafkaSourceUtil.getKafkaProperties(context);
    consumerTimeout = Integer.parseInt(kafkaProps.getProperty(
            KafkaSourceConstants.CONSUMER_TIMEOUT));
    kafkaAutoCommitEnabled = Boolean.parseBoolean(kafkaProps.getProperty(
            KafkaSourceConstants.AUTO_COMMIT_ENABLED));

  }

  @Override
  public synchronized void start() {
    log.info("Starting {}...", this);

    try {
      //initialize a consumer. This creates the connection to ZooKeeper
      consumer = KafkaSourceUtil.getConsumer(kafkaProps);
    } catch (Exception e) {
      throw new FlumeException("Unable to create consumer. " +
              "Check whether the ZooKeeper server is up and that the " +
              "Flume agent can connect to it.", e);
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    // We always have just one topic being read by one thread
    topicCountMap.put(topic, 1);

    // Get the message iterator for our topic
    // Note that this succeeds even if the topic doesn't exist
    // in that case we simply get no messages for the topic
    // Also note that currently we only support a single topic
    try {
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
              consumer.createMessageStreams(topicCountMap);
      List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
      KafkaStream<byte[], byte[]> stream = topicList.get(0);
      it = stream.iterator();
    } catch (Exception e) {
      throw new FlumeException("Unable to get message iterator from Kafka", e);
    }
    log.info("Kafka source {} started.", getName());
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (consumer != null) {
      // exit cleanly. This syncs offsets of messages read to ZooKeeper
      // to avoid reading the same messages again
      consumer.shutdown();
    }
    super.stop();
  }

  /**
   * Check if there are messages waiting in Kafka,
   * waiting until timeout (10ms by default) for messages to arrive.
   * and catching the timeout exception to return a boolean
   */
  boolean hasNext() {
    try {
      it.hasNext();
      return true;
    } catch (ConsumerTimeoutException e) {
      return false;
    }
  }

}