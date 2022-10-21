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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts an event from a local component and publishes it to a channel.
 */
public class LocalSource extends AbstractSource implements Configurable, EventDrivenSource, EventProcessor {

  private static final Logger logger = LoggerFactory.getLogger(LocalSource.class);

  private SourceCounter sourceCounter;

  /**
   * Called by flume to start this source.
   */
  @Override
  public void start() {
    logger.info("Local source {} starting.", getName());
    sourceCounter.start();
    super.start();
  }

  /**
   * Called by flume to stop this source.
   */
  @Override
  public void stop() {
    logger.info("Local source {} stopping.", getName());
    sourceCounter.stop();
    super.stop();
    logger.info("Local source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  /**
   * A message is passed in here. It is data that should be passed on.
   *
   * @param event The message.
   */
  public void processEvent(Event event) {
    if (event == null) {
      //Ignoring this.  Not counting as an event received either.
      return;
    }

    sourceCounter.incrementAppendReceivedCount();
    sourceCounter.incrementEventReceivedCount();
    logger.debug("pushing event to channel");
    getChannelProcessor().processEvent(event);
    sourceCounter.incrementAppendAcceptedCount();
    sourceCounter.incrementEventAcceptedCount();
  }

  /**
   * Called when flume starts up.
   *
   * @param context - Config values for this source from flume properties file.
   */
  @Override
  public void configure(Context context) {
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }
}
