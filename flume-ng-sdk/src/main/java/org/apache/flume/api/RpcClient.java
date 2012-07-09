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
package org.apache.flume.api;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;

/**
 * <p>Public client interface for sending data to Flume.</p>
 *
 * <p>This interface is intended not to change incompatibly for Flume 1.x.</p>
 *
 * <p><strong>Note:</strong> It is recommended for applications to construct
 * {@link RpcClient} instances using the {@link RpcClientFactory} class,
 * instead of using builders associated with a particular implementation class.
 * </p>
 *
 * @see org.apache.flume.api.RpcClientFactory
 */
public interface RpcClient {

  /**
   * Returns the maximum number of {@link Event events} that may be batched
   * at once by {@link #appendBatch(List) appendBatch()}.
   */
  public int getBatchSize();

  /**
   * <p>Send a single {@link Event} to the associated Flume source.</p>
   *
   * <p>This method blocks until the RPC returns or until the request times out.
   * </p>
   *
   * <p><strong>Note:</strong> If this method throws an
   * {@link EventDeliveryException}, there is no way to recover and the
   * application must invoke {@link #close()} on this object to clean up system
   * resources.</p>
   *
   * @param event
   * @return
   * @throws EventDeliveryException when an error prevents event delivery.
   */
  public void append(Event event) throws EventDeliveryException;

  /**
   * <p>Send a list of {@linkplain Event events} to the associated Flume source.
   * </p>
   *
   * <p>This method blocks until the RPC returns or until the request times out.
   * </p>
   *
   * <p>It is strongly recommended that the number of events in the List be no
   * more than {@link #getBatchSize()}. If it is more, multiple RPC calls will
   * be required, and the likelihood of duplicate Events being stored will
   * increase.</p>
   *
   * <p><strong>Note:</strong> If this method throws an
   * {@link EventDeliveryException}, there is no way to recover and the
   * application must invoke {@link #close()} on this object to clean up system
   * resources.</p>
   *
   * @param events List of events to send
   * @return
   * @throws EventDeliveryException when an error prevents event delivery.
   */
  public void appendBatch(List<Event> events) throws
      EventDeliveryException;

  /**
   * <p>Returns {@code true} if this object appears to be in a usable state, and
   * it returns {@code false} if this object is permanently disabled.</p>
   *
   * <p>If this method returns {@code false}, an application must call
   * {@link #close()} on this object to clean up system resources.</p>
   */
  public boolean isActive();

  /**
   * <p>Immediately closes the client so that it may no longer be used.</p>
   *
   * <p><strong>Note:</strong> This method MUST be called by applications
   * when they are done using the RPC client in order to clean up resources.</p>
   *
   * <p>Multi-threaded applications may want to gracefully stop making
   * RPC calls before calling close(). Otherwise, they risk getting
   * {@link EventDeliveryException} thrown from their in-flight calls when the
   * underlying connection is disabled.</p>
   */
  public void close() throws FlumeException;


}
