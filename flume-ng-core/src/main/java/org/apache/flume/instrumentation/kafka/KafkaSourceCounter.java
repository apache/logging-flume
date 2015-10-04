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

import org.apache.flume.instrumentation.SourceCounter;

public class KafkaSourceCounter extends SourceCounter implements  KafkaSourceCounterMBean {

  private static final String TIMER_KAFKA_EVENT_GET =
      "source.kafka.event.get.time";

  private static final String TIMER_KAFKA_COMMIT =
      "source.kafka.commit.time";

  private static final String COUNTER_KAFKA_EMPTY =
      "source.kafka.empty.count";

  private static final String[] ATTRIBUTES =
      {TIMER_KAFKA_COMMIT, TIMER_KAFKA_EVENT_GET, COUNTER_KAFKA_EMPTY};

  public KafkaSourceCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public long addToKafkaEventGetTimer(long delta) {
    return addAndGet(TIMER_KAFKA_EVENT_GET,delta);
  }

  public long addToKafkaCommitTimer(long delta) {
    return addAndGet(TIMER_KAFKA_COMMIT,delta);
  }

  public long incrementKafkaEmptyCount() {
    return increment(COUNTER_KAFKA_EMPTY);
  }

  public long getKafkaCommitTimer() {
    return get(TIMER_KAFKA_COMMIT);
  }

  public long getKafkaEventGetTimer() {
    return get(TIMER_KAFKA_EVENT_GET);
  }

  public long getKafkaEmptyCount() {
    return get(COUNTER_KAFKA_EMPTY);
  }

}
