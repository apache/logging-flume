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

import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.node.Initializable;
import org.apache.flume.node.MaterializedConfiguration;
import org.apache.flume.source.EventProcessor;
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
public class NullInitSink extends NullSink implements Initializable {

  private static final Logger logger = LoggerFactory.getLogger(NullInitSink.class);
  private String sourceName = null;
  private EventProcessor eventProcessor = null;
  private long total = 0;

  public NullInitSink() {
    super();
  }

  @Override
  public void configure(Context context) {
    sourceName = context.getString("targetSource");
    super.configure(context);

  }

  @Override
  public void initialize(MaterializedConfiguration configuration) {
    logger.debug("Locating source for event publishing");
    for (Map.Entry<String, SourceRunner>  entry : configuration.getSourceRunners().entrySet()) {
      if (entry.getKey().equals(sourceName)) {
        Source source = entry.getValue().getSource();
        if (source instanceof EventProcessor) {
          eventProcessor = (EventProcessor) source;
          logger.debug("Found event processor {}", source.getName());
          return;
        }
      }
    }
    logger.warn("No Source named {} found for republishing events.", sourceName);
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    CounterGroup counterGroup = getCounterGroup();
    long batchSize = getBatchSize();
    long eventCounter = counterGroup.get("events.success");

    try {
      transaction.begin();
      int i = 0;
      for (i = 0; i < batchSize; i++) {
        event = channel.take();
        if (event != null) {
          long id = Long.parseLong(new String(event.getBody()));
          total += id;
          event.getHeaders().put("Total", Long.toString(total));
          eventProcessor.processEvent(event);
          logger.info("Null sink {} successful processed event {}", getName(), id);
        } else {
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
}
