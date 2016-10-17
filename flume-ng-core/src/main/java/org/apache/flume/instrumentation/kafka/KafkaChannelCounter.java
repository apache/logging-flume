/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation.kafka;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flume.instrumentation.ChannelCounter;

public class KafkaChannelCounter extends ChannelCounter
    implements KafkaChannelCounterMBean {

  private static final String TIMER_KAFKA_EVENT_GET =
      "channel.kafka.event.get.time";

  private static final String TIMER_KAFKA_EVENT_SEND =
      "channel.kafka.event.send.time";

  private static final String TIMER_KAFKA_COMMIT =
      "channel.kafka.commit.time";

  private static final String COUNT_ROLLBACK =
      "channel.rollback.count";

  private static String[] ATTRIBUTES = {
    TIMER_KAFKA_COMMIT,TIMER_KAFKA_EVENT_SEND,TIMER_KAFKA_EVENT_GET, COUNT_ROLLBACK
  };

  public KafkaChannelCounter(String name) {
    super(name,ATTRIBUTES);
  }

  public long addToKafkaEventGetTimer(long delta) {
    return addAndGet(TIMER_KAFKA_EVENT_GET, delta);
  }

  public long addToKafkaEventSendTimer(long delta) {
    return addAndGet(TIMER_KAFKA_EVENT_SEND, delta);
  }

  public long addToKafkaCommitTimer(long delta) {
    return addAndGet(TIMER_KAFKA_COMMIT, delta);
  }

  public long addToRollbackCounter(long delta) {
    return addAndGet(COUNT_ROLLBACK,delta);
  }

  public long getKafkaEventGetTimer() {
    return get(TIMER_KAFKA_EVENT_GET);
  }

  public long getKafkaEventSendTimer() {
    return get(TIMER_KAFKA_EVENT_SEND);
  }

  public long getKafkaCommitTimer() {
    return get(TIMER_KAFKA_COMMIT);
  }

  public long getRollbackCount() {
    return get(COUNT_ROLLBACK);
  }

}
