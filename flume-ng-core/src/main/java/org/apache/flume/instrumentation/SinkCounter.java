/*
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.instrumentation;

public class SinkCounter extends MonitoredCounterGroup implements
    SinkCounterMBean {

  private static final String COUNTER_CONNECTION_CREATED =
      "sink.connection.creation.count";

  private static final String COUNTER_CONNECTION_CLOSED =
      "sink.connection.closed.count";

  private static final String COUNTER_CONNECTION_FAILED =
      "sink.connection.failed.count";

  private static final String COUNTER_BATCH_EMPTY =
      "sink.batch.empty";

  private static final String COUNTER_BATCH_UNDERFLOW =
      "sink.batch.underflow";

  private static final String COUNTER_BATCH_COMPLETE =
      "sink.batch.complete";

  private static final String COUNTER_EVENT_DRAIN_ATTEMPT =
      "sink.event.drain.attempt";

  private static final String COUNTER_EVENT_DRAIN_SUCCESS =
      "sink.event.drain.sucess";

  private static final String[] ATTRIBUTES = {
    COUNTER_CONNECTION_CREATED, COUNTER_CONNECTION_CLOSED,
    COUNTER_CONNECTION_FAILED, COUNTER_BATCH_EMPTY,
    COUNTER_BATCH_UNDERFLOW, COUNTER_BATCH_COMPLETE,
    COUNTER_EVENT_DRAIN_ATTEMPT, COUNTER_EVENT_DRAIN_SUCCESS
  };


  public SinkCounter(String name) {
    super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
  }

  @Override
  public long getConnectionCreatedCount() {
    return get(COUNTER_CONNECTION_CREATED);
  }

  public long incrementConnectionCreatedCount() {
    return increment(COUNTER_CONNECTION_CREATED);
  }

  @Override
  public long getConnectionClosedCount() {
    return get(COUNTER_CONNECTION_CLOSED);
  }

  public long incrementConnectionClosedCount() {
    return increment(COUNTER_CONNECTION_CLOSED);
  }

  @Override
  public long getConnectionFailedCount() {
    return get(COUNTER_CONNECTION_FAILED);
  }

  public long incrementConnectionFailedCount() {
    return increment(COUNTER_CONNECTION_FAILED);
  }

  @Override
  public long getBatchEmptyCount() {
    return get(COUNTER_BATCH_EMPTY);
  }

  public long incrementBatchEmptyCount() {
    return increment(COUNTER_BATCH_EMPTY);
  }

  @Override
  public long getBatchUnderflowCount() {
    return get(COUNTER_BATCH_UNDERFLOW);
  }

  public long incrementBatchUnderflowCount() {
    return increment(COUNTER_BATCH_UNDERFLOW);
  }

  @Override
  public long getBatchCompleteCount() {
    return get(COUNTER_BATCH_COMPLETE);
  }

  public long incrementBatchCompleteCount() {
    return increment(COUNTER_BATCH_COMPLETE);
  }

  @Override
  public long getEventDrainAttemptCount() {
    return get(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long incrementEventDrainAttemptCount() {
    return increment(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long addToEventDrainAttemptCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_ATTEMPT, delta);
  }

  @Override
  public long getEventDrainSuccessCount() {
    return get(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long incrementEventDrainSuccessCount() {
    return increment(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long addToEventDrainSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_SUCCESS, delta);
  }
}
