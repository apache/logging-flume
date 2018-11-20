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

package org.apache.flume.sink.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
  private static final TestUtil instance = new TestUtil();

  private KafkaLocal kafkaServer;
  private boolean externalServers = true;
  private String kafkaServerUrl;
  private String zkServerUrl;
  private int kafkaLocalPort;
  private Properties clientProps;
  private int zkLocalPort;
  private KafkaConsumer<String, String> consumer;
  private AdminClient adminClient;

  private TestUtil() {
    init();
  }

  public static TestUtil getInstance() {
    return instance;
  }

  private void init() {
    try {
      Properties settings = new Properties();
      InputStream in = Class.class.getResourceAsStream("/testutil.properties");
      if (in != null) {
        settings.load(in);
      }
      externalServers = "true".equalsIgnoreCase(settings.getProperty("external-servers"));
      if (externalServers) {
        kafkaServerUrl = settings.getProperty("kafka-server-url");
        zkServerUrl = settings.getProperty("zk-server-url");
      } else {
        String hostname = InetAddress.getLocalHost().getHostName();
        zkLocalPort = getNextPort();
        kafkaLocalPort = getNextPort();
        kafkaServerUrl = hostname + ":" + kafkaLocalPort;
        zkServerUrl = hostname + ":" + zkLocalPort;
      }
      clientProps = createClientProperties();
    } catch (Exception e) {
      logger.error("Unexpected error", e);
      throw new RuntimeException("Unexpected error", e);
    }
  }

  private boolean startEmbeddedKafkaServer() {
    Properties kafkaProperties = new Properties();
    Properties zkProperties = new Properties();

    logger.info("Starting kafka server.");
    try {
      //load properties
      zkProperties.load(Class.class.getResourceAsStream(
          "/zookeeper.properties"));

      //start local Zookeeper
      // override the Zookeeper client port with the generated one.
      zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
      new ZooKeeperLocal(zkProperties);

      logger.info("ZooKeeper instance is successfully started on port " +
          zkLocalPort);

      kafkaProperties.load(Class.class.getResourceAsStream(
          "/kafka-server.properties"));
      // override the Zookeeper url.
      kafkaProperties.setProperty("zookeeper.connect", getZkUrl());
      // override the Kafka server port
      kafkaProperties.setProperty("port", Integer.toString(kafkaLocalPort));
      kafkaServer = new KafkaLocal(kafkaProperties);
      kafkaServer.start();
      logger.info("Kafka Server is successfully started on port " + kafkaLocalPort);

      return true;

    } catch (Exception e) {
      logger.error("Error starting the Kafka Server.", e);
      return false;
    }
  }

  private AdminClient getAdminClient() {
    if (adminClient == null) {
      Properties adminClientProps = createAdminClientProperties();
      adminClient = AdminClient.create(adminClientProps);
    }
    return adminClient;
  }

  private Properties createClientProperties() {
    final Properties props = createAdminClientProperties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("consumer.timeout.ms","10000");
    props.put("max.poll.interval.ms","10000");

    // Create the consumer using props.
    return props;
  }

  private Properties createAdminClientProperties() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerUrl());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_1");
    return props;
  }

  public void initTopicList(List<String> topics) {
    consumer = new KafkaConsumer<>(clientProps);
    consumer.subscribe(topics);
  }

  public void createTopics(List<String> topicNames, int numPartitions) {
    List<NewTopic> newTopics = new ArrayList<>();
    for (String topicName: topicNames) {
      NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
      newTopics.add(newTopic);
    }
    getAdminClient().createTopics(newTopics);

    //the following lines are a bit of black magic to ensure the topic is ready when we return
    DescribeTopicsResult dtr = getAdminClient().describeTopics(topicNames);
    try {
      dtr.all().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException("Error getting topic info", e);
    }
  }
  public void deleteTopic(String topicName) {
    getAdminClient().deleteTopics(Collections.singletonList(topicName));
  }

  public ConsumerRecords<String, String> getNextMessageFromConsumer(String topic) {
    return consumer.poll(Duration.ofMillis(1000L));
  }

  public void prepare() {

    if (externalServers) {
      return;
    }
    boolean startStatus = startEmbeddedKafkaServer();
    if (!startStatus) {
      throw new RuntimeException("Error starting the server!");
    }
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    logger.info("Completed the prepare phase.");
  }

  public void tearDown() {
    logger.info("Shutting down the Kafka Consumer.");
    if (consumer != null) {
      consumer.close();
    }
    if (adminClient != null) {
      adminClient.close();
      adminClient = null;
    }
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    if (kafkaServer != null) {
      logger.info("Shutting down the kafka Server.");
      kafkaServer.stop();
    }
    logger.info("Completed the tearDown phase.");
  }

  private synchronized int getNextPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public String getZkUrl() {
    return zkServerUrl;
  }

  public String getKafkaServerUrl() {
    return kafkaServerUrl;
  }
}
