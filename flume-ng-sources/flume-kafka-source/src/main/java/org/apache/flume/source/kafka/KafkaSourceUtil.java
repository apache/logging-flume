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

import java.util.Map;
import java.util.Properties;

import kafka.common.KafkaException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceUtil {
  private static final Logger log =
          LoggerFactory.getLogger(KafkaSourceUtil.class);

  public static Properties getKafkaProperties(Context context) {
    log.info("context={}",context.toString());
    Properties props =  generateDefaultKafkaProps();
    setKafkaProps(context,props);
    addDocumentedKafkaProps(context,props);
    return props;
  }

  public static ConsumerConnector getConsumer(Properties kafkaProps) {
    ConsumerConfig consumerConfig =
            new ConsumerConfig(kafkaProps);
    ConsumerConnector consumer =
            Consumer.createJavaConsumerConnector(consumerConfig);
    return consumer;
  }

  /**
   * Generate consumer properties object with some defaults
   * @return
   */
  private static Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(KafkaSourceConstants.AUTO_COMMIT_ENABLED,
            KafkaSourceConstants.DEFAULT_AUTO_COMMIT);
    props.put(KafkaSourceConstants.CONSUMER_TIMEOUT,
            KafkaSourceConstants.DEFAULT_CONSUMER_TIMEOUT);
    props.put(KafkaSourceConstants.GROUP_ID,
            KafkaSourceConstants.DEFAULT_GROUP_ID);
    return props;
  }

  /**
   * Add all configuration parameters starting with "kafka"
   * to consumer properties
   */
  private static void setKafkaProps(Context context,Properties kafkaProps) {

    Map<String,String> kafkaProperties =
            context.getSubProperties(KafkaSourceConstants.PROPERTY_PREFIX);

    for (Map.Entry<String,String> prop : kafkaProperties.entrySet()) {

      kafkaProps.put(prop.getKey(), prop.getValue());
      if (log.isDebugEnabled()) {
        log.debug("Reading a Kafka Producer Property: key: "
                + prop.getKey() + ", value: " + prop.getValue());
      }
    }
  }

  /**
   * Some of the producer properties are especially important
   * We documented them and gave them a camel-case name to match Flume config
   * If user set these, we will override any existing parameters with these
   * settings.
   * Knowledge of which properties are documented is maintained here for now.
   * If this will become a maintenance issue we'll set a proper data structure.
   */
  private static void addDocumentedKafkaProps(Context context,
                                              Properties kafkaProps)
          throws ConfigurationException {
    String zookeeperConnect = context.getString(
            KafkaSourceConstants.ZOOKEEPER_CONNECT_FLUME);
    if (zookeeperConnect == null) {
      throw new ConfigurationException("ZookeeperConnect must contain " +
              "at least one ZooKeeper server");
    }
    kafkaProps.put(KafkaSourceConstants.ZOOKEEPER_CONNECT, zookeeperConnect);

    String groupID = context.getString(KafkaSourceConstants.GROUP_ID_FLUME);

    if (groupID != null ) {
      kafkaProps.put(KafkaSourceConstants.GROUP_ID, groupID);
    }
  }

}