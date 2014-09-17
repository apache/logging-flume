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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceUtil {
  private static final Logger log =
          LoggerFactory.getLogger(KafkaSourceUtil.class);

  public static Properties getKafkaConfigProperties(Context context) {
    log.info("context={}",context.toString());
    Properties props = new Properties();
    Map<String, String> contextMap = context.getParameters();
    for(String key : contextMap.keySet()) {
      String value = contextMap.get(key).trim();
      key = key.trim();
      if (key.startsWith(KafkaSourceConstants.PROPERTY_PREFIX)) {
      // remove the prefix
      key = key.substring(KafkaSourceConstants.PROPERTY_PREFIX.length() + 1,
              key.length());
        props.put(key, value);
        if (log.isDebugEnabled()) {
          log.debug("Reading a Kafka Producer Property: key: " + key +
                  ", value: " + value);
        }
      }
    }
    return props;
  }

  public static ConsumerConnector getConsumer(Context context) {
    ConsumerConfig consumerConfig =
            new ConsumerConfig(getKafkaConfigProperties(context));
    ConsumerConnector consumer =
            Consumer.createJavaConsumerConnector(consumerConfig);
    return consumer;
  }
}