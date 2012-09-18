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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
  private long maxTotalEvents;
  private long maxSuccessfulEvents;
  private int batchSize;
  private long lastSent = 0;
  private Event event;
  private List<Event> eventBatchList;
  private List<Event> eventBatchListToProcess;

  public StressSource() {
    counterGroup = new CounterGroup();

  }

  /**
   * Read parameters from context
   * <li>-maxTotalEvents = type long that defines the total number of events to be sent
   * <li>-maxSuccessfulEvents = type long that defines the total number of events to be sent
   * <li>-size = type int that defines the number of bytes in each event
   * <li>-batchSize = type int that defines the number of events being sent in one batch
   */
  @Override
  public void configure(Context context) {
    /* Limit on the total number of events. */
    maxTotalEvents = context.getLong("maxTotalEvents", -1L);
    /* Limit on the total number of successful events. */
    maxSuccessfulEvents = context.getLong("maxSuccessfulEvents", -1L);
    /* Set max events in a batch submission */
    batchSize = context.getInteger("batchSize", 1);
    /* Size of events to be generated. */
    int size = context.getInteger("size", 500);

    prepEventData(size);
  }

  private void prepEventData(int bufferSize) {
    buffer = new byte[bufferSize];
    Arrays.fill(buffer, Byte.MAX_VALUE);

    if (batchSize > 1) {
      //Create event objects in case of batch test
      eventBatchList = new ArrayList<Event>();

      for (int i = 0; i < batchSize; i++)
      {
        eventBatchList.add(EventBuilder.withBody(buffer));
      }
    } else {
      //Create single event in case of non-batch test
      event = EventBuilder.withBody(buffer);
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    long totalEventSent = counterGroup.addAndGet("events.total", lastSent);

    if ((maxTotalEvents >= 0 &&
        totalEventSent >= maxTotalEvents) ||
        (maxSuccessfulEvents >= 0 &&
        counterGroup.get("events.successful") >= maxSuccessfulEvents)) {
      return Status.BACKOFF;
    }
    try {
      lastSent = batchSize;

      if (batchSize == 1) {
        getChannelProcessor().processEvent(event);
      } else {
        long eventsLeft = maxTotalEvents - totalEventSent;

        if (maxTotalEvents >= 0 && eventsLeft < batchSize) {
          eventBatchListToProcess = eventBatchList.subList(0, (int)eventsLeft);
        } else {
          eventBatchListToProcess = eventBatchList;
        }
        lastSent = eventBatchListToProcess.size();
        getChannelProcessor().processEventBatch(eventBatchListToProcess);
      }

      counterGroup.addAndGet("events.successful", lastSent);
    } catch (ChannelException ex) {
      counterGroup.addAndGet("events.failed", lastSent);
      return Status.BACKOFF;
    }
    return Status.READY;
  }

  @Override
  public void start() {
    logger.info("Stress source starting");

    super.start();

    logger.debug("Stress source started");
  }

  @Override
  public void stop() {
    logger.info("Stress source stopping");

    super.stop();

    logger.info("Stress source stopped. Metrics:{}", counterGroup);
  }
}
