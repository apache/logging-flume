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
package org.apache.flume.interceptor;

import java.util.List;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Interceptor {
  /**
   * Any initialization / startup needed by the Interceptor.
   */
  public void initialize();

  /**
   * Interception of a single {@link Event}.
   * @param event Event to be intercepted
   * @return Original or modified event, or {@code null} if the Event
   * is to be dropped (i.e. filtered out).
   */
  public Event intercept(Event event);

  /**
   * Interception of a batch of {@linkplain Event events}.
   * @param events Input list of events
   * @return Output list of events. The size of output list MUST NOT BE GREATER
   * than the size of the input list (i.e. transformation and removal ONLY).
   * Also, this method MUST NOT return {@code null}. If all events are dropped,
   * then an empty List is returned.
   */
  public List<Event> intercept(List<Event> events);

  /**
   * Perform any closing / shutdown needed by the Interceptor.
   */
  public void close();

  /** Builder implementations MUST have a no-arg constructor */
  public interface Builder extends Configurable {
    public Interceptor build();
  }
}
