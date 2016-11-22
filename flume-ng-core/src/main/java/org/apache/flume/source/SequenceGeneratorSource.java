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

import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SequenceGeneratorSource extends AbstractPollableSource implements
        Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(SequenceGeneratorSource.class);

  private int batchSize;
  private SourceCounter sourceCounter;
  private long totalEvents;
  private long eventsSent = 0;

  /**
   * Read parameters from context
   * <li>batchSize = type int that defines the size of event batches
   */
  @Override
  protected void doConfigure(Context context) throws FlumeException {
    batchSize = context.getInteger("batchSize", 1);
    totalEvents = context.getLong("totalEvents", Long.MAX_VALUE);

    Preconditions.checkArgument(batchSize > 0, "batchSize was %s but expected positive", batchSize);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  protected Status doProcess() throws EventDeliveryException {
    Status status = Status.READY;
    long eventsSentTX = eventsSent;
    try {
      if (batchSize == 1) {
        if (eventsSentTX < totalEvents) {
          getChannelProcessor().processEvent(
                  EventBuilder.withBody(String.valueOf(eventsSentTX++).getBytes()));
          sourceCounter.incrementEventAcceptedCount();
        } else {
          status = Status.BACKOFF;
        }
      } else {
        List<Event> batchArrayList = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          if (eventsSentTX < totalEvents) {
            batchArrayList.add(i, EventBuilder.withBody(String
                    .valueOf(eventsSentTX++).getBytes()));
          } else {
            status = Status.BACKOFF;
            break;
          }
        }
        if (!batchArrayList.isEmpty()) {
          getChannelProcessor().processEventBatch(batchArrayList);
          sourceCounter.incrementAppendBatchAcceptedCount();
          sourceCounter.addToEventAcceptedCount(batchArrayList.size());
        }
      }
      eventsSent = eventsSentTX;
    } catch (ChannelException ex) {
      logger.error( getName() + " source could not write to channel.", ex);
    }

    return status;
  }

  @Override
  protected void doStart() throws FlumeException {
    logger.info("Sequence generator source do starting");
    sourceCounter.start();
    logger.debug("Sequence generator source do started");
  }

  @Override
  protected void doStop() throws FlumeException {
    logger.info("Sequence generator source do stopping");

    sourceCounter.stop();

    logger.info("Sequence generator source do stopped. Metrics:{}",getName(), sourceCounter);
  }

}
