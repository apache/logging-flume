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
package org.apache.flume.sink.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SinkCallbackTest {
  //note that this is log4j backing the slf4j logger
  //slf4j does not give an API to change log level at runtime
  private static final Logger SINK_BACKING_LOGGER = Logger.getLogger(SinkCallback.class);

  Level originalLevel;

  @Before
  public void setup() {
    originalLevel = SINK_BACKING_LOGGER.getLevel();
  }

  @After
  public void cleanup() {
    SINK_BACKING_LOGGER.setLevel(originalLevel);
  }

  private final SinkCallback callback = new SinkCallback(System.currentTimeMillis());
  private final RecordMetadata metadata
          = new RecordMetadata(new TopicPartition("TEST-TOPIC", 0), 0, 0);
  private final Exception exception = new Exception("TEST");

  @Test
  public void onCompletion() throws Exception {

    //fail cases
    Logger.getLogger(SinkCallback.class).setLevel(Level.DEBUG);
    callback.onCompletion(null, exception);
    Logger.getLogger(SinkCallback.class).setLevel(Level.WARN);
    callback.onCompletion(null, exception);

    //normal cases
    Logger.getLogger(SinkCallback.class).setLevel(Level.DEBUG);
    callback.onCompletion(metadata, null);
    Logger.getLogger(SinkCallback.class).setLevel(Level.WARN);
    callback.onCompletion(metadata, null);

  }

}