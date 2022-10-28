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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.shared.kafka.KafkaSSLUtil;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.apache.flume.shared.kafka.KafkaSSLUtil.SSL_DISABLE_FQDN_CHECK;
import static org.apache.flume.shared.kafka.KafkaSSLUtil.isSSLEnabled;

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
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
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
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * useFlumeEventFormat - preserves event headers when serializing onto Kafka
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable, BatchSizeSupported {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

  private final Properties kafkaProps = new Properties();
  private KafkaProducer<String, byte[]> producer;

  private String topic;
  private int batchSize;
  private List<Future<RecordMetadata>> kafkaFutures;
  private KafkaSinkCounter counter;
  private boolean useAvroEventFormat;
  private String partitionHeader = null;
  private Integer staticPartitionId = null;
  private boolean allowTopicOverride;
  private String topicHeader = null;
  private String timestampHeader = null;
  private Map<String, String> headerMap;

  private boolean useKafkaTransactions = false;

  private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional.absent();
  private Optional<ByteArrayOutputStream> tempOutStream = Optional.absent();

  //Fine to use null for initial value, Avro will create new ones if this
  // is null
  private BinaryEncoder encoder = null;


  //For testing
  public String getTopic() {
    return topic;
  }

  public long getBatchSize() {
    return batchSize;
  }

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
      if (useKafkaTransactions) {
        producer.beginTransaction();
      }

      kafkaFutures.clear();
      long batchStartTime = System.nanoTime();
      for (; processedEvents < batchSize; processedEvents += 1) {
        event = channel.take();

        if (event == null) {
          // no events available in channel
          if (processedEvents == 0) {
            result = Status.BACKOFF;
            counter.incrementBatchEmptyCount();
          } else {
            counter.incrementBatchUnderflowCount();
          }
          break;
        }
        counter.incrementEventDrainAttemptCount();

        byte[] eventBody = event.getBody();
        Map<String, String> headers = event.getHeaders();

        if (allowTopicOverride) {
          eventTopic = headers.get(topicHeader);
          if (eventTopic == null) {
            eventTopic = BucketPath.escapeString(topic, event.getHeaders());
            logger.debug("{} was set to true but header {} was null. Producing to {}" +
                " topic instead.",
                new Object[]{KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER,
                    topicHeader, eventTopic});
          }
        } else {
          eventTopic = topic;
        }

        eventKey = headers.get(KafkaSinkConstants.KEY_HEADER);
        if (logger.isTraceEnabled()) {
          if (LogPrivacyUtil.allowLogRawData()) {
            logger.trace("{Event} " + eventTopic + " : " + eventKey + " : "
                + new String(eventBody, StandardCharsets.UTF_8));
          } else {
            logger.trace("{Event} " + eventTopic + " : " + eventKey);
          }
        }
        logger.debug("event #{}", processedEvents);

        // create a message and add to buffer
        long startTime = System.currentTimeMillis();

        Integer partitionId = null;
        try {
          ProducerRecord<String, byte[]> record;
          if (staticPartitionId != null) {
            partitionId = staticPartitionId;
          }
          //Allow a specified header to override a static ID
          if (partitionHeader != null) {
            String headerVal = event.getHeaders().get(partitionHeader);
            if (headerVal != null) {
              partitionId = Integer.parseInt(headerVal);
            }
          }
          Long timestamp = null;
          if (timestampHeader != null) {
            String value = headers.get(timestampHeader);
            if (value != null) {
              try {
                timestamp = Long.parseLong(value);
              } catch (Exception ex) {
                logger.warn("Invalid timestamp in header {} - {}", timestampHeader, value);
              }
            }
          }
          List<Header> kafkaHeaders = null;
          if (!headerMap.isEmpty()) {
            List<Header> tempHeaders = new ArrayList<>();
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
              String value = headers.get(entry.getKey());
              if (value != null) {
                tempHeaders.add(new RecordHeader(entry.getValue(),
                    value.getBytes(StandardCharsets.UTF_8)));
              }
            }
            if (!tempHeaders.isEmpty()) {
              kafkaHeaders = tempHeaders;
            }
          }

          if (partitionId != null) {
            record = new ProducerRecord<>(eventTopic, partitionId, timestamp, eventKey,
                serializeEvent(event, useAvroEventFormat), kafkaHeaders);
          } else {
            record = new ProducerRecord<>(eventTopic, null, timestamp, eventKey,
                serializeEvent(event, useAvroEventFormat), kafkaHeaders);
          }
          kafkaFutures.add(producer.send(record, new SinkCallback(startTime)));
        } catch (NumberFormatException ex) {
          throw new EventDeliveryException("Non integer partition id specified", ex);
        } catch (Exception ex) {
          // N.B. The producer.send() method throws all sorts of RuntimeExceptions
          // Catching Exception here to wrap them neatly in an EventDeliveryException
          // which is what our consumers will expect
          throw new EventDeliveryException("Could not send event", ex);
        }
      }

      if (useKafkaTransactions) {
        producer.commitTransaction();
      } else {
        //Prevent linger.ms from holding the batch
        producer.flush();
        for (Future<RecordMetadata> future : kafkaFutures) {
          future.get();
        }
      }
      // publish batch and commit.
      if (processedEvents > 0) {
        long endTime = System.nanoTime();
        counter.addToKafkaEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
        counter.addToEventDrainSuccessCount(processedEvents);
      }

      transaction.commit();

    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      counter.incrementEventWriteOrChannelFail(ex);
      if (transaction != null) {
        try {
          kafkaFutures.clear();
          try {
            if (useKafkaTransactions) {
              producer.abortTransaction();
            }
          } catch (ProducerFencedException e) {
            logger.error("Could not rollback transaction as producer fenced", e);
          } finally {
            transaction.rollback();
            counter.incrementRollbackCount();
          }
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
    producer = new KafkaProducer<>(kafkaProps);
    if (useKafkaTransactions) {
      logger.info("Transactions enabled, initializing transactions");
      producer.initTransactions();
    }
    counter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    producer.close();
    counter.stop();
    logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
    super.stop();
  }


  /**
   * We configure the sink and generate properties for the Kafka Producer
   *
   * Kafka producer properties is generated as follows:
   * 1. We generate a properties object with some static defaults that
   * can be overridden by Sink configuration
   * 2. We add the configuration users added for Kafka (parameters starting
   * with .kafka. and must be valid Kafka Producer properties
   * 3. We add the sink's documented parameters which can override other
   * properties
   *
   * @param context The Context.
   */
  @Override
  public void configure(Context context) {

    String topicStr = context.getString(KafkaSinkConstants.TOPIC_CONFIG);
    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = KafkaSinkConstants.DEFAULT_TOPIC;
      logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
    } else {
      logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
    }

    topic = topicStr;

    timestampHeader = context.getString(KafkaSinkConstants.TIMESTAMP_HEADER);

    headerMap = context.getSubProperties(KafkaSinkConstants.KAFKA_HEADER);

    batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);

    if (logger.isDebugEnabled()) {
      logger.debug("Using batch size: {}", batchSize);
    }

    useAvroEventFormat = context.getBoolean(KafkaSinkConstants.AVRO_EVENT,
                                            KafkaSinkConstants.DEFAULT_AVRO_EVENT);

    partitionHeader = context.getString(KafkaSinkConstants.PARTITION_HEADER_NAME);
    staticPartitionId = context.getInteger(KafkaSinkConstants.STATIC_PARTITION_CONF);

    allowTopicOverride = context.getBoolean(KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER,
                                          KafkaSinkConstants.DEFAULT_ALLOW_TOPIC_OVERRIDE_HEADER);

    topicHeader = context.getString(KafkaSinkConstants.TOPIC_OVERRIDE_HEADER,
                                    KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER);

    String transactionalID = context.getString(KafkaSinkConstants.TRANSACTIONAL_ID);
    if (transactionalID != null) {
      try {
        context.put(KafkaSinkConstants.TRANSACTIONAL_ID, InetAddress.getLocalHost().getCanonicalHostName() +
                Thread.currentThread().getName() + transactionalID);
        useKafkaTransactions = true;
      } catch (UnknownHostException e) {
        throw new ConfigurationException("Unable to configure transactional id, as cannot work out hostname", e);
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug(KafkaSinkConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
    }

    kafkaFutures = new LinkedList<Future<RecordMetadata>>();

    String bootStrapServers = context.getString(KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG);
    if (bootStrapServers == null || bootStrapServers.isEmpty()) {
      throw new ConfigurationException("Bootstrap Servers must be specified");
    }

    setProducerProps(context, bootStrapServers);

    if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
      logger.debug("Kafka producer properties: {}", kafkaProps);
    }

    if (counter == null) {
      counter = new KafkaSinkCounter(getName());
    }
  }

  private void setProducerProps(Context context, String bootStrapServers) {
    kafkaProps.clear();
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, KafkaSinkConstants.DEFAULT_ACKS);
    //Defaults overridden based on config
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaSinkConstants.DEFAULT_VALUE_SERIAIZER);
    kafkaProps.putAll(context.getSubProperties(KafkaSinkConstants.KAFKA_PRODUCER_PREFIX));
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    //  The default value of `ssl.endpoint.identification.algorithm`
    //  is changed to `https`, since kafka client 2.0+
    //  And because flume does not accept an empty string as property value,
    //  so we need to use an alternative custom property
    //  `ssl.disableTLSHostnameVerification` to check if enable fqdn check.
    if (isSSLEnabled(kafkaProps) && "true".equalsIgnoreCase(kafkaProps.getProperty(SSL_DISABLE_FQDN_CHECK))) {
      kafkaProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }
    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);
  }

  protected Properties getKafkaProps() {
    return kafkaProps;
  }

  private byte[] serializeEvent(Event event, boolean useAvroEventFormat) throws IOException {
    byte[] bytes;
    if (useAvroEventFormat) {
      if (!tempOutStream.isPresent()) {
        tempOutStream = Optional.of(new ByteArrayOutputStream());
      }
      if (!writer.isPresent()) {
        writer = Optional.of(new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
      }
      tempOutStream.get().reset();
      AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()),
                                            ByteBuffer.wrap(event.getBody()));
      encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream.get(), encoder);
      writer.get().write(e, encoder);
      encoder.flush();
      bytes = tempOutStream.get().toByteArray();
    } else {
      bytes = event.getBody();
    }
    return bytes;
  }

  private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

}

class SinkCallback implements Callback {
  private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
  private long startTime;

  public SinkCallback(long startTime) {
    this.startTime = startTime;
  }

  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      logger.warn("Error sending message to Kafka {} ", exception.getMessage());
    }

    if (logger.isDebugEnabled()) {
      long eventElapsedTime = System.currentTimeMillis() - startTime;
      if (metadata != null) {
        logger.debug("Acked message partition:{} ofset:{}", metadata.partition(),
                metadata.offset());
      }
      logger.debug("Elapsed time for send: {}", eventElapsedTime);
    }
  }
}

