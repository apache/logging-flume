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

package org.apache.flume.tools;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

/**
 * Event Validator interface to be used for validating Events
 * per custom logic
 */
public interface EventValidator {

  /**
   * Validate the Event in a application specific manner
   *
   * @param event Flume Event
   * @return  true if Event is valid as per App Logic
   */
  boolean validateEvent(Event event);

  EventValidator NOOP_VALIDATOR = new EventValidator() {
    @Override
    public boolean validateEvent(Event event) {
      return true;
    }
  };

  interface Builder extends Configurable {
    EventValidator build();
  }
}
