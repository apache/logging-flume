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

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;

import java.util.List;

/**
 * Interface for an event serializer which serializes the headers and body
 * of an event to write them to Kudu. This is configurable, so any config
 * params required should be taken through this. The columns should exist
 * in the table specified in the configuration for the KuduSink.
 */
public interface KuduEventSerializer extends Configurable, ConfigurableComponent {
  /**
   * Initialize the event serializer.
   * @param event to be written to Kudu.
   * @param table
   */
  void initialize(Event event, KuduTable table);

  /**
   * Get the actions that should be written out to Kudu as a result of this
   * event. This list is written to Kudu using the Kudu client API.
   * @return List of {@link org.kududb.client.PartialRow} which
   * are written as such to Kudu.
   */
  List<Operation> getOperations();

  /*
   * Clean up any state. This will be called when the sink is being stopped.
   */
  void close();
}
