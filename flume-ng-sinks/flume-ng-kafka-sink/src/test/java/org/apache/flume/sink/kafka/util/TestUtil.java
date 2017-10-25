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

import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
  private static TestUtil instance = new TestUtil();

  private KafkaLocal kafkaServer;
  private KafkaConsumer kafkaConsumer;
  private String hostname = "localhost";
  private int kafkaLocalPort;
  private int zkLocalPort;

  private TestUtil() {
    init();
  }

  public static TestUtil getInstance() {
    return instance;
  }

  private void init() {
    // get the localhost.
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.warn("Error getting the value of localhost. " +
          "Proceeding with 'localhost'.", e);
    }
  }

  private boolean startKafkaServer() {
    Properties kafkaProperties = new Properties();
    Properties zkProperties = new Properties();

    logger.info("Starting kafka server.");
    try {
      //load properties
      zkProperties.load(Class.class.getResourceAsStream(
          "/zookeeper.properties"));

      //start local Zookeeper
      zkLocalPort = getNextPort();
      // override the Zookeeper client port with the generated one.
      zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
      new ZooKeeperLocal(zkProperties);

      logger.info("ZooKeeper instance is successfully started on port " +
          zkLocalPort);

      kafkaProperties.load(Class.class.getResourceAsStream(
          "/kafka-server.properties"));
      // override the Zookeeper url.
      kafkaProperties.setProperty("zookeeper.connect", getZkUrl());
      kafkaLocalPort = getNextPort();
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

  private KafkaConsumer getKafkaConsumer() {
    synchronized (this) {
      if (kafkaConsumer == null) {
        kafkaConsumer = new KafkaConsumer();
      }
    }
    return kafkaConsumer;
  }

  public void initTopicList(List<String> topics) {
    getKafkaConsumer().initTopicList(topics);
  }

  public MessageAndMetadata getNextMessageFromConsumer(String topic) {
    return getKafkaConsumer().getNextMessage(topic);
  }

  public void prepare() {
    boolean startStatus = startKafkaServer();
    if (!startStatus) {
      throw new RuntimeException("Error starting the server!");
    }
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    getKafkaConsumer();
    logger.info("Completed the prepare phase.");
  }

  public void tearDown() {
    logger.info("Shutting down the Kafka Consumer.");
    getKafkaConsumer().shutdown();
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    logger.info("Shutting down the kafka Server.");
    kafkaServer.stop();
    logger.info("Completed the tearDown phase.");
  }

  private synchronized int getNextPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public String getZkUrl() {
    return hostname + ":" + zkLocalPort;
  }

  public String getKafkaServerUrl() {
    return hostname + ":" + kafkaLocalPort;
  }
}
