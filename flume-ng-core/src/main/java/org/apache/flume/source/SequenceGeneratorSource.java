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
import java.util.List;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceGeneratorSource extends AbstractSource implements
    PollableSource, Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(SequenceGeneratorSource.class);

  private long sequence;
  private int batchSize;
  private SourceCounter sourceCounter;
  private List<Event> batchArrayList;
  private long totalEvents;
  private long eventsSent = 0;

  public SequenceGeneratorSource() {
    sequence = 0;
  }

  /**
   * Read parameters from context
   * <li>batchSize = type int that defines the size of event batches
   */
  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", 1);
    if (batchSize > 1) {
      batchArrayList = new ArrayList<Event>(batchSize);
    }
    totalEvents = context.getLong("totalEvents", Long.MAX_VALUE);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public Status process() throws EventDeliveryException {

    Status status = Status.READY;
    int i = 0;
    try {
      if (batchSize <= 1) {
        if(eventsSent < totalEvents) {
          getChannelProcessor().processEvent(
            EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
          sourceCounter.incrementEventAcceptedCount();
          eventsSent++;
        } else {
          status = Status.BACKOFF;
        }
      } else {
        batchArrayList.clear();
        for (i = 0; i < batchSize; i++) {
          if(eventsSent < totalEvents){
            batchArrayList.add(i, EventBuilder.withBody(String
              .valueOf(sequence++).getBytes()));
            eventsSent++;
          } else {
            status = Status.BACKOFF;
          }
        }
        if(!batchArrayList.isEmpty()) {
          getChannelProcessor().processEventBatch(batchArrayList);
          sourceCounter.incrementAppendBatchAcceptedCount();
          sourceCounter.addToEventAcceptedCount(batchArrayList.size());
        }
      }

    } catch (ChannelException ex) {
      eventsSent -= i;
      logger.error( getName() + " source could not write to channel.", ex);
    }

    return status;
  }

  @Override
  public void start() {
    logger.info("Sequence generator source starting");

    super.start();
    sourceCounter.start();
    logger.debug("Sequence generator source started");
  }

  @Override
  public void stop() {
    logger.info("Sequence generator source stopping");

    super.stop();
    sourceCounter.stop();

    logger.info("Sequence generator source stopped. Metrics:{}",getName(), sourceCounter);
  }

}
