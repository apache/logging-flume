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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.flume.Context;
import org.apache.zookeeper.server.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKafkaSourceUtil {
  private Properties props = new Properties();
  private Context context = new Context();
  private int zkPort = 21818; // none-standard
  private KafkaSourceEmbeddedZookeeper zookeeper;

  @Before
  public void setUp() throws Exception {
    context.put("kafka.consumer.timeout", "10");
    context.put("type", "KafkaSource");
    context.put("topic", "test");
    context.put("zookeeperConnect", "127.0.0.1:"+zkPort);
    context.put("groupId","test");
    props = KafkaSourceUtil.getKafkaProperties(context);
    zookeeper = new KafkaSourceEmbeddedZookeeper(zkPort);


  }

  @After
  public void tearDown() throws Exception {
    zookeeper.stopZookeeper();
  }


  @Test
  public void testGetConsumer() {
    ConsumerConnector cc = KafkaSourceUtil.getConsumer(props);
    assertNotNull(cc);

  }

  @Test
  public void testKafkaConsumerProperties() {
    Context context = new Context();
    context.put("kafka.auto.commit.enable", "override.default.autocommit");
    context.put("kafka.fake.property", "kafka.property.value");
    context.put("kafka.zookeeper.connect","bad-zookeeper-list");
    context.put("zookeeperConnect","real-zookeeper-list");
    Properties kafkaProps = KafkaSourceUtil.getKafkaProperties(context);

    //check that we have defaults set
    assertEquals(
            kafkaProps.getProperty(KafkaSourceConstants.GROUP_ID),
            KafkaSourceConstants.DEFAULT_GROUP_ID);
    //check that kafka properties override the default and get correct name
    assertEquals(
            kafkaProps.getProperty(KafkaSourceConstants.AUTO_COMMIT_ENABLED),
            "override.default.autocommit");
    //check that any kafka property gets in
    assertEquals(kafkaProps.getProperty("fake.property"),
            "kafka.property.value");
    //check that documented property overrides defaults
    assertEquals(kafkaProps.getProperty("zookeeper.connect")
            ,"real-zookeeper-list");
  }


}
