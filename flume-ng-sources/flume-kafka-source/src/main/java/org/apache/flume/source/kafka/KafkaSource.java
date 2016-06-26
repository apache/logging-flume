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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * A Source for Kafka which reads messages from kafka topics.
 *
 * <tt>kafka.bootstrap.servers: </tt> A comma separated list of host:port pairs
 * to use for establishing the initial connection to the Kafka cluster.
 * For example host1:port1,host2:port2,...
 * <b>Required</b> for kafka.
 * <p>
 * <tt>kafka.consumer.group.id: </tt> the group ID of consumer group. <b>Required</b>
 * <p>
 * <tt>kafka.topics: </tt> the topic list separated by commas to consume messages from.
 * <b>Required</b>
 * <p>
 * <tt>maxBatchSize: </tt> Maximum number of messages written to Channel in one
 * batch. Default: 1000
 * <p>
 * <tt>maxBatchDurationMillis: </tt> Maximum number of milliseconds before a
 * batch (of any size) will be written to a channel. Default: 1000
 * <p>
 * <tt>kafka.consumer.*: </tt> Any property starting with "kafka.consumer" will be
 * passed to the kafka consumer So you can use any configuration supported by Kafka 0.9.0.X
 * <tt>useFlumeEventFormat: </tt> Reads events from Kafka Topic as an Avro FlumeEvent. Used
 * in conjunction with useFlumeEventFormat (Kafka Sink) or parseAsFlumeEvent (Kafka Channel)
 * <p>
 */
public class KafkaSource extends AbstractPollableSource
        implements Configurable {
  private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

  private Context context;
  private Properties kafkaProps;
  private KafkaSourceCounter counter;
  private KafkaConsumer<String, byte[]> consumer;
  private Iterator<ConsumerRecord<String, byte[]>> it;

  private final List<Event> eventList = new ArrayList<Event>();
  private Map<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata;
  private AtomicBoolean rebalanceFlag;

  private Map<String, String> headers;

  private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
  private BinaryDecoder decoder = null;

  private boolean useAvroEventFormat;

  private int batchUpperLimit;
  private int maxBatchDurationMillis;

  private Subscriber subscriber;


  /**
   * This class is a helper to subscribe for topics by using
   * different strategies
   */
  public abstract class Subscriber<T> {
    public abstract void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener);

    public T get() {
      return null;
    }
  }

  private class TopicListSubscriber extends Subscriber<List<String>> {
    private List<String> topicList;

    public TopicListSubscriber(String commaSeparatedTopics) {
      this.topicList = Arrays.asList(commaSeparatedTopics.split("^\\s+|\\s*,\\s*|\\s+$"));
    }

    @Override
    public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
      consumer.subscribe(topicList, listener);
    }

    @Override
    public List<String> get() {
      return topicList;
    }
  }

  private class PatternSubscriber extends Subscriber<Pattern> {
    private Pattern pattern;

    public PatternSubscriber(String regex) {
      this.pattern = Pattern.compile(regex);
    }

    @Override
    public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
      consumer.subscribe(pattern, listener);
    }

    @Override
    public Pattern get() {
      return pattern;
    }
  }


  @Override
  protected Status doProcess() throws EventDeliveryException {
    final String batchUUID = UUID.randomUUID().toString();
    byte[] kafkaMessage;
    String kafkaKey;
    Event event;
    byte[] eventBody;

    try {
      // prepare time variables for new batch
      final long nanoBatchStartTime = System.nanoTime();
      final long batchStartTime = System.currentTimeMillis();
      final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

      while (eventList.size() < batchUpperLimit &&
              System.currentTimeMillis() < maxBatchEndTime) {

        if (it == null || !it.hasNext()) {
          // Obtaining new records
          // Poll time is remainder time for current batch.
          ConsumerRecords<String, byte[]> records = consumer.poll(
                  Math.max(0, maxBatchEndTime - System.currentTimeMillis()));
          it = records.iterator();

          // this flag is set to true in a callback when some partitions are revoked.
          // If there are any records we commit them.
          if (rebalanceFlag.get()) {
            rebalanceFlag.set(false);
            break;
          }
          // check records after poll
          if (!it.hasNext()) {
            if (log.isDebugEnabled()) {
              counter.incrementKafkaEmptyCount();
              log.debug("Returning with backoff. No more data to read");
            }
            // batch time exceeded
            break;
          }
        }

        // get next message
        ConsumerRecord<String, byte[]> message = it.next();
        kafkaKey = message.key();
        kafkaMessage = message.value();

        if (useAvroEventFormat) {
          //Assume the event is in Avro format using the AvroFlumeEvent schema
          //Will need to catch the exception if it is not
          ByteArrayInputStream in =
                  new ByteArrayInputStream(message.value());
          decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
          if (!reader.isPresent()) {
            reader = Optional.of(
                    new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
          }
          //This may throw an exception but it will be caught by the
          //exception handler below and logged at error
          AvroFlumeEvent avroevent = reader.get().read(null, decoder);

          eventBody = avroevent.getBody().array();
          headers = toStringMap(avroevent.getHeaders());
        } else {
          eventBody = message.value();
          headers.clear();
          headers = new HashMap<String, String>(4);
        }

        // Add headers to event (timestamp, topic, partition, key) only if they don't exist
        if (!headers.containsKey(KafkaSourceConstants.TIMESTAMP_HEADER)) {
          headers.put(KafkaSourceConstants.TIMESTAMP_HEADER,
              String.valueOf(System.currentTimeMillis()));
        }
        if (!headers.containsKey(KafkaSourceConstants.TOPIC_HEADER)) {
          headers.put(KafkaSourceConstants.TOPIC_HEADER, message.topic());
        }
        if (!headers.containsKey(KafkaSourceConstants.PARTITION_HEADER)) {
          headers.put(KafkaSourceConstants.PARTITION_HEADER,
              String.valueOf(message.partition()));
        }

        if (kafkaKey != null) {
          headers.put(KafkaSourceConstants.KEY_HEADER, kafkaKey);
        }

        if (log.isDebugEnabled()) {
          log.debug("Topic: {} Partition: {} Message: {}", new String[] {
              message.topic(),
              String.valueOf(message.partition()),
              new String(eventBody)
          });
        }

        event = EventBuilder.withBody(eventBody, headers);
        eventList.add(event);

        if (log.isDebugEnabled()) {
          log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
          log.debug("Event #: {}", eventList.size());
        }

        // For each partition store next offset that is going to be read.
        tpAndOffsetMetadata.put(new TopicPartition(message.topic(), message.partition()),
                new OffsetAndMetadata(message.offset() + 1, batchUUID));
      }

      if (eventList.size() > 0) {
        counter.addToKafkaEventGetTimer((System.nanoTime() - nanoBatchStartTime) / (1000 * 1000));
        counter.addToEventReceivedCount((long) eventList.size());
        getChannelProcessor().processEventBatch(eventList);
        counter.addToEventAcceptedCount(eventList.size());
        if (log.isDebugEnabled()) {
          log.debug("Wrote {} events to channel", eventList.size());
        }
        eventList.clear();

        if (!tpAndOffsetMetadata.isEmpty()) {
          long commitStartTime = System.nanoTime();
          consumer.commitSync(tpAndOffsetMetadata);
          long commitEndTime = System.nanoTime();
          counter.addToKafkaCommitTimer((commitEndTime - commitStartTime) / (1000 * 1000));
          tpAndOffsetMetadata.clear();
        }
        return Status.READY;
      }

      return Status.BACKOFF;
    } catch (Exception e) {
      log.error("KafkaSource EXCEPTION, {}", e);
      return Status.BACKOFF;
    }
  }

  /**
   * We configure the source and generate properties for the Kafka Consumer
   *
   * Kafka Consumer properties are generated as follows:
   * 1. Generate a properties object with some static defaults that can be
   * overridden if corresponding properties are specified
   * 2. We add the configuration users added for Kafka (parameters starting
   * with kafka.consumer and must be valid Kafka Consumer properties
   * 3. Add source level properties (with no prefix)
   * @param context
   */
  @Override
  protected void doConfigure(Context context) throws FlumeException {
    this.context = context;
    headers = new HashMap<String, String>(4);
    tpAndOffsetMetadata = new HashMap<TopicPartition, OffsetAndMetadata>();
    rebalanceFlag = new AtomicBoolean(false);
    kafkaProps = new Properties();

    // can be removed in the next release
    // See https://issues.apache.org/jira/browse/FLUME-2896
    translateOldProperties(context);

    String topicProperty = context.getString(KafkaSourceConstants.TOPICS_REGEX);
    if (topicProperty != null && !topicProperty.isEmpty()) {
      // create subscriber that uses pattern-based subscription
      subscriber = new PatternSubscriber(topicProperty);
    } else if ((topicProperty = context.getString(KafkaSourceConstants.TOPICS)) != null &&
               !topicProperty.isEmpty()) {
      // create subscriber that uses topic list subscription
      subscriber = new TopicListSubscriber(topicProperty);
    } else if (subscriber == null) {
      throw new ConfigurationException("At least one Kafka topic must be specified.");
    }

    batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
                                         KafkaSourceConstants.DEFAULT_BATCH_SIZE);
    maxBatchDurationMillis = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
                                                KafkaSourceConstants.DEFAULT_BATCH_DURATION);

    useAvroEventFormat = context.getBoolean(KafkaSourceConstants.AVRO_EVENT,
                                            KafkaSourceConstants.DEFAULT_AVRO_EVENT);

    if (log.isDebugEnabled()) {
      log.debug(KafkaSourceConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
    }

    String bootstrapServers = context.getString(KafkaSourceConstants.BOOTSTRAP_SERVERS);
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      throw new ConfigurationException("Bootstrap Servers must be specified");
    }

    setConsumerProps(context, bootstrapServers);

    if (counter == null) {
      counter = new KafkaSourceCounter(getName());
    }
  }

  // We can remove this once the properties are officially deprecated
  private void translateOldProperties(Context ctx) {
    // topic
    String topic = context.getString(KafkaSourceConstants.TOPIC);
    if (topic != null && !topic.isEmpty()) {
      subscriber = new TopicListSubscriber(topic);
      log.warn("{} is deprecated. Please use the parameter {}",
              KafkaSourceConstants.TOPIC, KafkaSourceConstants.TOPICS);
    }

    // old groupId
    String groupId = ctx.getString(KafkaSourceConstants.OLD_GROUP_ID);
    if (groupId != null && !groupId.isEmpty()) {
      kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      log.warn("{} is deprecated. Please use the parameter {}",
              KafkaSourceConstants.OLD_GROUP_ID,
              KafkaSourceConstants.KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
    }
  }

  private void setConsumerProps(Context ctx, String bootStrapServers) {
    String groupId = ctx.getString(
        KafkaSourceConstants.KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
    if ((groupId == null || groupId.isEmpty()) &&
        kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG) == null) {
      groupId = KafkaSourceConstants.DEFAULT_GROUP_ID;
      log.info("Group ID was not specified. Using " + groupId + " as the group id.");
    }
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                   KafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   KafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
    //Defaults overridden based on config
    kafkaProps.putAll(ctx.getSubProperties(KafkaSourceConstants.KAFKA_CONSUMER_PREFIX));
    //These always take precedence over config
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    if (groupId != null) {
      kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                   KafkaSourceConstants.DEFAULT_AUTO_COMMIT);

    log.info(kafkaProps.toString());
  }

  Properties getConsumerProps() {
    return kafkaProps;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  <T> Subscriber<T> getSubscriber() {
    return subscriber;
  }

  @Override
  protected void doStart() throws FlumeException {
    log.info("Starting {}...", this);

    //initialize a consumer.
    consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

    // Subscribe for topics by already specified strategy
    subscriber.subscribe(consumer, new SourceRebalanceListener(rebalanceFlag));

    // Connect to kafka. 1 second is optimal time.
    it = consumer.poll(1000).iterator();
    log.info("Kafka source {} started.", getName());
    counter.start();
  }

  @Override
  protected void doStop() throws FlumeException {
    if (consumer != null) {
      consumer.wakeup();
      consumer.close();
    }
    counter.stop();
    log.info("Kafka Source {} stopped. Metrics: {}", getName(), counter);
  }
}

class SourceRebalanceListener implements ConsumerRebalanceListener {
  private static final Logger log = LoggerFactory.getLogger(SourceRebalanceListener.class);
  private AtomicBoolean rebalanceFlag;

  public SourceRebalanceListener(AtomicBoolean rebalanceFlag) {
    this.rebalanceFlag = rebalanceFlag;
  }

  // Set a flag that a rebalance has occurred. Then commit already read events to kafka.
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
      rebalanceFlag.set(true);
    }
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
    }
  }
}
