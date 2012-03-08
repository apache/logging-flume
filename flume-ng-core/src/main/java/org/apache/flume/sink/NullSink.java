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

import org.apache.flume.Channel;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
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
public class NullSink extends AbstractSink {

  private static final Logger logger = LoggerFactory.getLogger(NullSink.class);

  private CounterGroup counterGroup;

  public NullSink() {
    counterGroup = new CounterGroup();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;

    try {
      transaction.begin();
      event = channel.take();
      transaction.commit();

      if (event != null) {
        // logger.debug("Consumed the event: " + event);
        counterGroup.incrementAndGet("events.successful");
        return Status.READY;
      } else {
        return Status.BACKOFF;
      }
    } catch (Exception ex) {
      transaction.rollback();
      counterGroup.incrementAndGet("events.failed");
      logger.error("Failed to deliver event. Exception follows.", ex);
      throw new EventDeliveryException("Failed to deliver event: " + event, ex);
    } finally {
      transaction.close();
    }
  }

  @Override
  public void start() {
    logger.info("Null sink starting");

    super.start();

    logger.debug("Null sink started");
  }

  @Override
  public void stop() {
    logger.info("Null sink stopping");

    super.stop();

    logger.debug("Null sink stopped. Event metrics:{}", counterGroup);
  }

}
