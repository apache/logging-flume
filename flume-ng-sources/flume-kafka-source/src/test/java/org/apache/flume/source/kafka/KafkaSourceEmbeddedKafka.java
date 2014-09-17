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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkClient;
import kafka.utils.ZKStringSerializer$;

import java.io.IOException;
import java.util.Properties;

public class KafkaSourceEmbeddedKafka {
  KafkaServerStartable kafkaServer;
  KafkaSourceEmbeddedZookeeper zookeeper;
  int zkPort = 21818; // none-standard
  Producer<String,String> producer;

  public KafkaSourceEmbeddedKafka() {
    zookeeper = new KafkaSourceEmbeddedZookeeper(zkPort);
    Properties props = new Properties();
    props.put("zookeeper.connect",zookeeper.getConnectString());
    props.put("broker.id","1");
    KafkaConfig config = new KafkaConfig(props);
    kafkaServer = new KafkaServerStartable(config);
    kafkaServer.startup();
    initProducer();
  }

  public void stop() throws IOException {
    producer.close();
    kafkaServer.shutdown();
    zookeeper.stopZookeeper();
  }

  public String getZkConnectString() {
    return zookeeper.getConnectString();
  }

  private void initProducer()
  {
    Properties props = new Properties();
    props.put("metadata.broker.list","127.0.0.1:" +
            kafkaServer.serverConfig().port());
    props.put("serializer.class","kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    producer = new Producer<String,String>(config);

  }

  public void produce(String topic, String k, String v) {
    KeyedMessage<String,String> message = new KeyedMessage<String,String>(topic,k,v);
    producer.send(message);
  }

  public void createTopic(String topicName) {
    // Create a ZooKeeper client
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(zookeeper.getConnectString(),
            sessionTimeoutMs, connectionTimeoutMs,
            ZKStringSerializer$.MODULE$);

    int numPartitions = 1;
    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkClient, topicName, numPartitions,
            replicationFactor, topicConfig);
  }

}
