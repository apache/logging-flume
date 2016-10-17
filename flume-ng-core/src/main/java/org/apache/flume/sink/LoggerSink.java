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

import com.google.common.base.Strings;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link org.apache.flume.Sink} implementation that logs all events received at the INFO level
 * to the <tt>org.apache.flume.sink.LoggerSink</tt> logger.
 * </p>
 * <p>
 * <b>WARNING:</b> Logging events can potentially introduce performance
 * degradation.
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
public class LoggerSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(LoggerSink.class);

  // Default Max bytes to dump
  public static final int DEFAULT_MAX_BYTE_DUMP = 16;

  // Max number of bytes to be dumped
  private int maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;

  public static final String MAX_BYTES_DUMP_KEY = "maxBytesToLog";

  @Override
  public void configure(Context context) {
    String strMaxBytes = context.getString(MAX_BYTES_DUMP_KEY);
    if (!Strings.isNullOrEmpty(strMaxBytes)) {
      try {
        maxBytesToLog = Integer.parseInt(strMaxBytes);
      } catch (NumberFormatException e) {
        logger.warn(String.format(
            "Unable to convert %s to integer, using default value(%d) for maxByteToDump",
            strMaxBytes, DEFAULT_MAX_BYTE_DUMP));
        maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;

    try {
      transaction.begin();
      event = channel.take();

      if (event != null) {
        if (logger.isInfoEnabled()) {
          logger.info("Event: " + EventHelper.dumpEvent(event, maxBytesToLog));
        }
      } else {
        // No event found, request back-off semantics from the sink runner
        result = Status.BACKOFF;
      }
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to log event: " + event, ex);
    } finally {
      transaction.close();
    }

    return result;
  }
}
