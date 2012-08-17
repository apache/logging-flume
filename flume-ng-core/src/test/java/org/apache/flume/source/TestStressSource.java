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

package org.apache.flume.source;

import static org.fest.reflect.core.Reflection.field;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.Before;
import org.junit.Test;

public class TestStressSource {

  private ChannelProcessor mockProcessor;

  @Before
  public void setUp() {
    mockProcessor = mock(ChannelProcessor.class);
  }

  private Event getEvent(StressSource source) {
    return field("event").ofType(Event.class)
          .in(source)
          .get();
  }

  @Test
  public void testMaxTotalEvents() throws InterruptedException,
      EventDeliveryException {
    StressSource source = new StressSource();
    source.setChannelProcessor(mockProcessor);
    Context context = new Context();
    context.put("maxTotalEvents", "35");
    source.configure(context);

    for (int i = 0; i < 50; i++) {
      source.process();
    }
    verify(mockProcessor, times(35)).processEvent(getEvent(source));
  }

  @Test
  public void testMaxSuccessfulEvents() throws InterruptedException,
      EventDeliveryException {
    StressSource source = new StressSource();
    source.setChannelProcessor(mockProcessor);
    Context context = new Context();
    context.put("maxSuccessfulEvents", "35");
    source.configure(context);

    for (int i = 0; i < 10; i++) {
      source.process();
    }

    // 1 failed call, 10 successful
    doThrow(new ChannelException("stub")).when(
        mockProcessor).processEvent(getEvent(source));
    source.process();
    doNothing().when(mockProcessor).processEvent(getEvent(source));
    for (int i = 0; i < 10; i++) {
      source.process();
    }

    // 1 failed call, 50 succesful
    doThrow(new ChannelException("stub")).when(
        mockProcessor).processEvent(getEvent(source));
    source.process();
    doNothing().when(mockProcessor).processEvent(getEvent(source));
    for (int i = 0; i < 50; i++) {
      source.process();
    }

    // We should have called processEvent(evt) 37 times, twice for failures
    // and twice for successful events.
    verify(mockProcessor, times(37)).processEvent(getEvent(source));
  }
}
