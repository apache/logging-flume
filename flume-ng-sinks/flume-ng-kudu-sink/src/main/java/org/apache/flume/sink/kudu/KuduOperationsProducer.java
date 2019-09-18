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

package org.apache.flume.sink.kudu;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

/**
 * Interface for an operations producer that produces Kudu Operations from
 * Flume events.
 */
public interface KuduOperationsProducer extends Configurable, AutoCloseable {
  /**
   * Initializes the operations producer. Called between configure and
   * getOperations.
   * @param table the KuduTable used to create Kudu Operation objects
   */
  void initialize(KuduTable table);

  /**
   * Returns the operations that should be written to Kudu as a result of this event.
   * @param event Event to convert to one or more Operations
   * @return List of Operations that should be written to Kudu
   */
  List<Operation> getOperations(Event event);

  /**
   * Cleans up any state. Called when the sink is stopped.
   */
  @Override
  void close();
}
