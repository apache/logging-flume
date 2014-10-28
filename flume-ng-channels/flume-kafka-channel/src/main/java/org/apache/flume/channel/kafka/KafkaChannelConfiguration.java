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

public class KafkaChannelConfiguration {

  public static final String KAFKA_PREFIX = "kafka.";
  public static final String BROKER_LIST_KEY = "metadata.broker.list";
  public static final String REQUIRED_ACKS_KEY = "request.required.acks";
  public static final String BROKER_LIST_FLUME_KEY = "brokerList";
  public static final String TOPIC = "topic";
  public static final String GROUP_ID = "group.id";
  public static final String GROUP_ID_FLUME = "groupId";
  public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
  public static final String DEFAULT_GROUP_ID = "flume";
  public static final String DEFAULT_TOPIC = "flume-channel";
  public static final String TIMEOUT = "timeout";
  public static final String DEFAULT_TIMEOUT = "100";
  public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";

  public static final String PARSE_AS_FLUME_EVENT = "parseAsFlumeEvent";
  public static final boolean DEFAULT_PARSE_AS_FLUME_EVENT = true;

  public static final String READ_SMALLEST_OFFSET = "readSmallestOffset";
  public static final boolean DEFAULT_READ_SMALLEST_OFFSET = false;
}
