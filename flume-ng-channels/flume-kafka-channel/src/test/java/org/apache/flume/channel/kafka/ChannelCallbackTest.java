/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChannelCallbackTest {

  //note that this is log4j backing the slf4j logger
  //slf4j does not give an API to change log level at runtime
  private static final Logger CHANNEL_BACKING_LOGGER = Logger.getLogger(ChannelCallback.class);

  Level originalLevel;

  @Before
  public void setup() {
    originalLevel = CHANNEL_BACKING_LOGGER.getLevel();
  }

  @After
  public void cleanup() {
    CHANNEL_BACKING_LOGGER.setLevel(originalLevel);
  }

  private final ChannelCallback channelCallback = new ChannelCallback(0, System.currentTimeMillis());
  private final RecordMetadata metadata
          = new RecordMetadata(new TopicPartition("TEST-TOPIC", 0), 0, 0);
  private final Exception exception = new Exception("TEST");

  @Test
  public void onCompletion() throws Exception {
    //fail cases
    CHANNEL_BACKING_LOGGER.setLevel(Level.DEBUG);
    channelCallback.onCompletion(null, exception);
    CHANNEL_BACKING_LOGGER.setLevel(Level.WARN);
    channelCallback.onCompletion(null, exception);

    //normal cases
    CHANNEL_BACKING_LOGGER.setLevel(Level.DEBUG);
    channelCallback.onCompletion(metadata, null);
    CHANNEL_BACKING_LOGGER.setLevel(Level.WARN);
    channelCallback.onCompletion(metadata, null);

  }

}