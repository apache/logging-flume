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

package org.apache.flume.sink.kafka;

import junit.framework.TestCase;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaSinkUtilTest extends TestCase {

  @Test
  public void testGetKafkaProperties() {
    Context context = new Context();
    context.put("kafka.serializer.class", "override.default.serializer");
    context.put("kafka.fake.property", "kafka.property.value");
    context.put("kafka.metadata.broker.list","bad-broker-list");
    context.put("brokerList","real-broker-list");
    Properties kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

    //check that we have defaults set
    assertEquals(
            kafkaProps.getProperty(KafkaSinkConstants.KEY_SERIALIZER_KEY),
            KafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
    //check that kafka properties override the default and get correct name
    assertEquals(
            kafkaProps.getProperty(KafkaSinkConstants.MESSAGE_SERIALIZER_KEY),
            "override.default.serializer");
    //check that any kafka property gets in
    assertEquals(kafkaProps.getProperty("fake.property"),
            "kafka.property.value");
    //check that documented property overrides defaults
    assertEquals(kafkaProps.getProperty("metadata.broker.list")
            ,"real-broker-list");
  }
}