/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

public class ChannelCounter extends MonitoredCounterGroup implements
    ChannelCounterMBean {

  private static final String COUNTER_CHANNEL_SIZE = "channel.current.size";

  private static final String COUNTER_EVENT_PUT_ATTEMPT =
      "channel.event.put.attempt";

  private static final String COUNTER_EVENT_TAKE_ATTEMPT =
      "channel.event.take.attempt";

  private static final String COUNTER_EVENT_PUT_SUCCESS =
      "channel.event.put.success";

  private static final String COUNTER_EVENT_TAKE_SUCCESS =
      "channel.event.take.success";

  private static final String COUNTER_CHANNEL_CAPACITY =
          "channel.capacity";

  private static final String[] ATTRIBUTES = {
    COUNTER_CHANNEL_SIZE, COUNTER_EVENT_PUT_ATTEMPT,
    COUNTER_EVENT_TAKE_ATTEMPT, COUNTER_EVENT_PUT_SUCCESS,
    COUNTER_EVENT_TAKE_SUCCESS, COUNTER_CHANNEL_CAPACITY
  };

  public ChannelCounter(String name) {
    super(MonitoredCounterGroup.Type.CHANNEL, name, ATTRIBUTES);
  }

  @Override
  public long getChannelSize() {
    return get(COUNTER_CHANNEL_SIZE);
  }

  public void setChannelSize(long newSize) {
    set(COUNTER_CHANNEL_SIZE, newSize);
  }

  @Override
  public long getEventPutAttemptCount() {
    return get(COUNTER_EVENT_PUT_ATTEMPT);
  }

  public long incrementEventPutAttemptCount() {
    return increment(COUNTER_EVENT_PUT_ATTEMPT);
  }

  @Override
  public long getEventTakeAttemptCount() {
    return get(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  public long incrementEventTakeAttemptCount() {
    return increment(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  @Override
  public long getEventPutSuccessCount() {
    return get(COUNTER_EVENT_PUT_SUCCESS);
  }

  public long addToEventPutSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_PUT_SUCCESS, delta);
  }

  @Override
  public long getEventTakeSuccessCount() {
    return get(COUNTER_EVENT_TAKE_SUCCESS);
  }

  public long addToEventTakeSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_TAKE_SUCCESS, delta);
  }

  public void setChannelCapacity(long capacity){
    set(COUNTER_CHANNEL_CAPACITY, capacity);
  }

  @Override
  public long getChannelCapacity(){
    return get(COUNTER_CHANNEL_CAPACITY);
  }

  @Override
  public double getChannelFillPercentage(){
    long capacity = getChannelCapacity();
    if(capacity != 0L) {
      return ((getChannelSize()/(double)capacity) * 100);
    }
    return Double.MAX_VALUE;
  }

}
