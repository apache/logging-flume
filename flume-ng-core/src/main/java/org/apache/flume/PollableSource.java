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

package org.apache.flume;

import org.apache.flume.source.EventDrivenSourceRunner;

/**
 * A {@link Source} that requires an external driver to poll to determine
 * whether there are {@linkplain Event events} that are available to ingest
 * from the source.
 *
 * @see org.apache.flume.source.EventDrivenSourceRunner
 */
public interface PollableSource extends Source {
  /**
   * <p>
   * Attempt to pull an item from the source, sending it to the channel.
   * </p>
   * <p>
   * When driven by an {@link EventDrivenSourceRunner} process is guaranteed
   * to be called only by a single thread at a time, with no concurrency.
   * Any other mechanism driving a pollable source must follow the same
   * semantics.
   * </p>
   * @return {@code READY} if one or more events were created from the source.
   * {@code BACKOFF} if no events could be created from the source.
   * @throws EventDeliveryException If there was a failure in delivering to
   * the attached channel, or if a failure occurred in acquiring data from
   * the source.
   */
  public Status process() throws EventDeliveryException;

  public long getBackOffSleepIncrement();

  public long getMaxBackOffSleepInterval();

  public static enum Status {
    READY, BACKOFF
  }

}
