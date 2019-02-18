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
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaSourceEmbeddedKafka {

  public static String HOST = InetAddress.getLoopbackAddress().getCanonicalHostName();

  KafkaServerStartable kafkaServer;
  KafkaSourceEmbeddedZookeeper zookeeper;
  private AdminClient adminClient;

  private static int findFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new AssertionError("Can not find free port.", e);
    }
  }

  private int zkPort = findFreePort(); // none-standard
  private int serverPort = findFreePort();

  KafkaProducer<String, byte[]> producer;
  File dir;

  public KafkaSourceEmbeddedKafka(Properties properties) {
    zookeeper = new KafkaSourceEmbeddedZookeeper(zkPort);
    dir = new File(System.getProperty("java.io.tmpdir"), "kafka_log-" + UUID.randomUUID());
    try {
      FileUtils.deleteDirectory(dir);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Properties props = new Properties();
    props.put("zookeeper.connect",zookeeper.getConnectString());
    props.put("broker.id","1");
    props.put("host.name", "localhost");
    props.put("port", String.valueOf(serverPort));
    props.put("log.dir", dir.getAbsolutePath());
    props.put("offsets.topic.replication.factor", "1");
    props.put("auto.create.topics.enable", "false");
    if (properties != null) {
      props.putAll(properties);
    }
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

  public String getBootstrapServers() {
    return HOST + ":" + serverPort;
  }

  private void initProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", HOST + ":" + serverPort);
    props.put("acks", "1");
    producer = new KafkaProducer<String,byte[]>(props,
            new StringSerializer(), new ByteArraySerializer());
  }

  public void produce(String topic, String k, String v) {
    produce(topic, k, v.getBytes());
  }

  public void produce(String topic, String k, byte[] v) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<String, byte[]>(topic, k, v);
    try {
      producer.send(rec).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void produce(String topic, int partition, String k, String v) {
    produce(topic,partition,k,v.getBytes());
  }

  public void produce(String topic, int partition, String k, byte[] v) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<String, byte[]>(topic, partition, k, v);
    try {
      producer.send(rec).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void createTopic(String topicName, int numPartitions) {
    AdminClient adminClient = getAdminClient();
    NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
    adminClient.createTopics(Collections.singletonList(newTopic));

    //the following lines are a bit of black magic to ensure the topic is ready when we return
    DescribeTopicsResult dtr = adminClient.describeTopics(Collections.singletonList(topicName));
    try {
      dtr.all().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException("Error getting topic info", e);
    }
  }

  private AdminClient getAdminClient() {
    if (adminClient == null) {
      final Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ":" + serverPort);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_1");
      adminClient = AdminClient.create(props);
    }
    return adminClient;
  }

  public void deleteTopics(List<String> topic) {
    getAdminClient().deleteTopics(topic);
  }

}
