/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka;

import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * kafka.metadata.broker.list -- can be a partial list,
 * but at least 2 are recommended for HA
 * kafka.request.required.acks -- 0 (unsafe), 1 (accepted by at least one
 * broker), -1 (accepted by all brokers)
 * kafka.producer.type -- for safety, this should be sync
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
  public static final String KEY_HDR = "key";
  public static final String TOPIC_HDR = "topic";
  private Properties producerProps;
  private Producer<String, byte[]> producer;
  private String topic;
  private int batchSize;
  private List<KeyedMessage<String, byte[]>> messageList;

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    String eventTopic = null;
    String eventKey = null;

    try {
      long processedEvents = 0;

      transaction = channel.getTransaction();
      transaction.begin();

      messageList.clear();
      for (; processedEvents < batchSize; processedEvents += 1) {
        event = channel.take();

        if (event == null) {
          // no events available in channel
          break;
        }

        byte[] eventBody = event.getBody();
        Map<String, String> headers = event.getHeaders();

        if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
          eventTopic = topic;
        }

        eventKey = headers.get(KEY_HDR);

        if (logger.isDebugEnabled()) {
          logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
            + new String(eventBody, "UTF-8"));
          logger.debug("event #{}", processedEvents);
        }

        // create a message and add to buffer
        KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>
          (eventTopic, eventKey, eventBody);
        messageList.add(data);

      }

      // publish batch and commit.
      if (processedEvents > 0) {
        producer.send(messageList);
      }

      transaction.commit();

    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
      throw new EventDeliveryException(errorMsg, ex);
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return result;
  }

  @Override
  public synchronized void start() {
    // instantiate the producer
    ProducerConfig config = new ProducerConfig(producerProps);
    producer = new Producer<String, byte[]>(config);
    super.start();
  }

  @Override
  public synchronized void stop() {
    producer.close();
    super.stop();
  }


  @Override
  public void configure(Context context) {

    batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE,
      KafkaSinkConstants.DEFAULT_BATCH_SIZE);
    logger.debug("Using batch size: {}", batchSize);
    messageList =
      new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
    Map<String, String> params = context.getParameters();
    logger.debug("all params: " + params.entrySet().toString());
    setProducerProps(params);
    if (!producerProps.contains("serializer.class")) {
      producerProps.put("serializer.class", "kafka.serializer.DefaultEncoder");
    }
    if (!producerProps.contains("key.serializer.class")) {
      producerProps.put("key.serializer.class",
        "kafka.serializer.StringEncoder");
    }

    topic = context.getString(KafkaSinkConstants.TOPIC,
      KafkaSinkConstants.DEFAULT_TOPIC);
    if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
      logger.warn("The Properties 'preprocessor' or 'topic' is not set. " +
        "Using the default topic name" +
        KafkaSinkConstants.DEFAULT_TOPIC);
    } else {
      logger.info("Using the static topic: " + topic +
        " this may be over-ridden by event headers");
    }
  }


  private void setProducerProps(Map<String, String> params) {
    producerProps = new Properties();
    for (String key : params.keySet()) {
      String value = params.get(key).trim();
      key = key.trim();
      if (key.startsWith(KafkaSinkConstants.PROPERTY_PREFIX)) {
        // remove the prefix
        key = key.substring(KafkaSinkConstants.PROPERTY_PREFIX.length() + 1,
          key.length());
        producerProps.put(key.trim(), value);
        if (logger.isDebugEnabled()) {
          logger.debug("Reading a Kafka Producer Property: key: " + key +
            ", value: " + value);
        }
      }
    }
  }

}
