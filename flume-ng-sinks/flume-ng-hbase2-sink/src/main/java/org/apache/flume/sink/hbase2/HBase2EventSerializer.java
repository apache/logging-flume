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
package org.apache.flume.sink.hbase2;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

/**
 * Interface for an event serializer which serializes the headers and body
 * of an event to write them to HBase 2. This is configurable, so any config
 * params required should be taken through this. Only the column family is
 * passed in. The columns should exist in the table and column family
 * specified in the configuration for the HBase2Sink.
 */
public interface HBase2EventSerializer extends Configurable, ConfigurableComponent {
  /**
   * Initialize the event serializer.
   * @param event Event to be written to HBase
   * @param columnFamily Column family to write to
   */
  void initialize(Event event, byte[] columnFamily);

  /**
   * Get the actions that should be written out to hbase as a result of this
   * event. This list is written to HBase using the HBase batch API.
   * @return List of {@link org.apache.hadoop.hbase.client.Row} which
   * are written as such to HBase.
   *
   * 0.92 increments do not implement Row, so this is not generic.
   *
   */
  List<Row> getActions();

  List<Increment> getIncrements();

  /*
   * Clean up any state. This will be called when the sink is being stopped.
   */
  void close();
}
