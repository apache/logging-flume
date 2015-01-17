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

import org.apache.flume.instrumentation.SinkCounter;

public class KafkaSinkCounter extends SinkCounter implements KafkaSinkCounterMBean {

  private static final String TIMER_KAFKA_EVENT_SEND =
      "channel.kafka.event.send.time";

  private static final String COUNT_ROLLBACK =
      "channel.rollback.count";

  private static final String[] ATTRIBUTES =
      {COUNT_ROLLBACK,TIMER_KAFKA_EVENT_SEND};

  public KafkaSinkCounter(String name) {
    super(name,ATTRIBUTES);
  }

  public long addToKafkaEventSendTimer(long delta) {
    return addAndGet(TIMER_KAFKA_EVENT_SEND,delta);
  }

  public long incrementRollbackCount() {
    return increment(COUNT_ROLLBACK);
  }

  public long getKafkaEventSendTimer() {
    return get(TIMER_KAFKA_EVENT_SEND);
  }

  public long getRollbackCount() {
    return get(COUNT_ROLLBACK);
  }

}
