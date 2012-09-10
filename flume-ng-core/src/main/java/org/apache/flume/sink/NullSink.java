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
package org.apache.flume.sink;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link Sink} implementation that simply discards all events it receives. A
 * <tt>/dev/null</tt> for Flume.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <p>
 * <i>This sink has no configuration parameters.</i>
 * </p>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class NullSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(NullSink.class);

  private static final int DFLT_BATCH_SIZE = 100;
  private static final int DFLT_LOG_EVERY_N_EVENTS = 10000;

  private CounterGroup counterGroup;
  private int batchSize = DFLT_BATCH_SIZE;
  private int logEveryNEvents = DFLT_LOG_EVERY_N_EVENTS;

  public NullSink() {
    counterGroup = new CounterGroup();
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", DFLT_BATCH_SIZE);
    logger.debug(this.getName() + " " +
        "batch size set to " + String.valueOf(batchSize));
    Preconditions.checkArgument(batchSize > 0, "Batch size must be > 0");

    logEveryNEvents = context.getInteger("logEveryNEvents", DFLT_LOG_EVERY_N_EVENTS);
    logger.debug(this.getName() + " " +
        "log event N events set to " + logEveryNEvents);
    Preconditions.checkArgument(logEveryNEvents > 0, "logEveryNEvents must be > 0");
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    long eventCounter = counterGroup.get("events.success");

    try {
      transaction.begin();
      int i = 0;
      for (i = 0; i < batchSize; i++) {
        event = channel.take();
        if (++eventCounter % logEveryNEvents == 0) {
          logger.info("Null sink {} successful processed {} events.", getName(), eventCounter);
        }
        if(event == null) {
          status = Status.BACKOFF;
          break;
        }
      }
      transaction.commit();
      counterGroup.addAndGet("events.success", (long) Math.min(batchSize, i));
      counterGroup.incrementAndGet("transaction.success");
    } catch (Exception ex) {
      transaction.rollback();
      counterGroup.incrementAndGet("transaction.failed");
      logger.error("Failed to deliver event. Exception follows.", ex);
      throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    } finally {
      transaction.close();
    }

    return status;
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    counterGroup.setName(this.getName());
    super.start();

    logger.info("Null sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("Null sink {} stopping...", getName());

    super.stop();

    logger.info("Null sink {} stopped. Event metrics: {}",
        getName(), counterGroup);
  }

  @Override
  public String toString() {
    return "NullSink " + getName() + " { batchSize: " + batchSize + " }";
  }

}
