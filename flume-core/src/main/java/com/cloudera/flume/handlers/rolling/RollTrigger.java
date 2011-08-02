/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.rolling;

import com.cloudera.flume.core.Event;

/**
 * A trigger is a condition for controlling when rolling happens in a RollSink.
 * The original default only allowed for time based triggers, but this interface
 * allows us to trigger on other events such as a certain size threshold, or
 * amount of time since last append, to do nagling, etc.
 */
public interface RollTrigger {

  /**
   * A check to see if the trigger is satisfied
   */
  boolean isTriggered();

  /**
   * A hook that allows the trigger to gather information from event that pass
   * through it. (potentially for counts, size aggregates, etc).
   */
  void append(Event e);

  /**
   * This is a tagger that generates a new unique tag name. It is used by the
   * RollSink to get a new name for the next batch.
   */
  Tagger getTagger();

  /**
   * Reset the trigger condition.
   */
  void reset();

}
