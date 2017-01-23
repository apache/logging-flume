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

import org.apache.kafka.clients.CommonClientConfigs;

public class KafkaSinkConstants {

  public static final String KAFKA_PREFIX = "kafka.";
  public static final String KAFKA_PRODUCER_PREFIX = KAFKA_PREFIX + "producer.";

  /* Properties */

  public static final String TOPIC_CONFIG = KAFKA_PREFIX + "topic";
  public static final String BATCH_SIZE = "flumeBatchSize";
  public static final String BOOTSTRAP_SERVERS_CONFIG =
      KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

  public static final String KEY_HEADER = "key";
  public static final String TOPIC_HEADER = "topic";

  public static final String AVRO_EVENT = "useFlumeEventFormat";
  public static final boolean DEFAULT_AVRO_EVENT = false;

  public static final String PARTITION_HEADER_NAME = "partitionIdHeader";
  public static final String STATIC_PARTITION_CONF = "defaultPartitionId";

  public static final String DEFAULT_KEY_SERIALIZER =
      "org.apache.kafka.common.serialization.StringSerializer";
  public static final String DEFAULT_VALUE_SERIAIZER =
      "org.apache.kafka.common.serialization.ByteArraySerializer";

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final String DEFAULT_TOPIC = "default-flume-topic";
  public static final String DEFAULT_ACKS = "1";

  /* Old Properties */

  /* Properties */

  public static final String OLD_BATCH_SIZE = "batchSize";
  public static final String MESSAGE_SERIALIZER_KEY = "serializer.class";
  public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
  public static final String BROKER_LIST_FLUME_KEY = "brokerList";
  public static final String REQUIRED_ACKS_FLUME_KEY = "requiredAcks";
}

