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

import java.util.Arrays;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal load-generating source implementation. Useful for tests.
 *
 * See {@link StressSource#configure(Context)} for configuration options.
 */
public class StressSource extends AbstractSource implements
  Configurable, PollableSource {

  private static final Logger logger = LoggerFactory
      .getLogger(StressSource.class);

  private CounterGroup counterGroup;
  private byte[] buffer;
  private Event event;
  private long maxTotalEvents;
  private long maxSuccessfulEvents;

  public StressSource() {
    counterGroup = new CounterGroup();

  }
  @Override
  public void configure(Context context) {
    /* Limit on the total number of events. */
    maxTotalEvents = context.getLong("maxTotalEvents", -1L);
    /* Limit on the total number of successful events. */
    maxSuccessfulEvents = context.getLong("maxSuccessfulEvents", -1L);
    /* Size of events to be generated. */
    int size = context.getInteger("size", 500);
    buffer = new byte[size];
    Arrays.fill(buffer, Byte.MAX_VALUE);
    event = EventBuilder.withBody(buffer);
  }
  @Override
  public Status process() throws EventDeliveryException {
    if ((maxTotalEvents >= 0 &&
        counterGroup.incrementAndGet("events.total") > maxTotalEvents) ||
        (maxSuccessfulEvents >= 0 &&
        counterGroup.get("events.successful") >= maxSuccessfulEvents)) {
      return Status.BACKOFF;
    }
    try {
      getChannelProcessor().processEvent(event);
      counterGroup.incrementAndGet("events.successful");
    } catch (ChannelException ex) {
      counterGroup.incrementAndGet("events.failed");
      return Status.BACKOFF;
    }
    return Status.READY;
  }

  @Override
  public void start() {
    logger.info("Sequence generator source starting");

    super.start();

    logger.debug("Sequence generator source started");
  }

  @Override
  public void stop() {
    logger.info("Sequence generator source stopping");

    super.stop();

    logger.info("Sequence generator source stopped. Metrics:{}", counterGroup);
  }
}
