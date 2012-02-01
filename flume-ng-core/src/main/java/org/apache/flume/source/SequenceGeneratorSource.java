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

import org.apache.flume.ChannelException;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceGeneratorSource extends AbstractSource implements
    PollableSource {

  private static final Logger logger = LoggerFactory
      .getLogger(SequenceGeneratorSource.class);

  private long sequence;
  private CounterGroup counterGroup;

  public SequenceGeneratorSource() {
    sequence = 0;
    counterGroup = new CounterGroup();
  }

  @Override
  public Status process() throws EventDeliveryException {

    try {
      getChannelProcessor().processEvent(
          EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
      counterGroup.incrementAndGet("events.successful");
    } catch (ChannelException ex) {
      counterGroup.incrementAndGet("events.failed");
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
