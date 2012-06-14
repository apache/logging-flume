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
package org.apache.flume.sink.hbase;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

public interface AsyncHbaseEventSerializer extends Configurable,
ConfigurableComponent {

  public void initialize(byte[] table, byte[] cf);

  /**
   * Initialize the event serializer.
   * @param Event to be written to HBase.
   */
  public void setEvent(Event event);

  /**
   * Get the actions that should be written out to hbase as a result of this
   * event. This list is written to hbase using the HBase batch API.
   * @return List of {@link org.apache.hadoop.hbase.client.Row} which
   * are written as such to HBase.
   *
   * 0.92 increments do not implement Row, so this is not generic.
   *
   */
  public List<PutRequest> getActions();

  public List<AtomicIncrementRequest> getIncrements();

  /*
   * Clean up any state. This will be called when the sink is being stopped.
   */
  public void cleanUp();
}
