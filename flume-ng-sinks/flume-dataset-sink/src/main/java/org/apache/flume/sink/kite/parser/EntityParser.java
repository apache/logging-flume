/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.sink.kite.parser;

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.kite.NonRecoverableEventException;

@NotThreadSafe
public interface EntityParser<E> {

  /**
   * Parse a Kite entity from a Flume event
   *
   * @param event The event to parse
   * @param reuse If non-null, this may be reused and returned
   * @return The parsed entity
   * @throws EventDeliveryException A recoverable error during parsing. Parsing
   *                                can be safely retried.
   * @throws NonRecoverableEventException A non-recoverable error during
   *                                      parsing. The event must be discarded.
   *                                    
   */
  public E parse(Event event, E reuse) throws EventDeliveryException,
      NonRecoverableEventException;

  /**
   * Knows how to build {@code EntityParser}s. Implementers must provide a
   * no-arg constructor.
   * 
   * @param <E> The type of entities generated
   */
  public static interface Builder<E> {

    public EntityParser<E> build(Schema datasetSchema, Context config);
  }
}
