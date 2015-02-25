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

package org.apache.flume.sink.kite.policy;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.kite.DatasetSink;
import org.kitesdk.data.Syncable;

/**
 * A policy for dealing with non-recoverable event delivery failures.
 *
 * Non-recoverable event delivery failures include:
 *
 * 1. Error parsing the event body thrown from the {@link EntityParser}
 * 2. A schema mismatch between the schema of an event and the schema of the
 *    destination dataset.
 * 3. A missing schema from the Event header when using the
 *    {@link AvroEntityParser}.
 *
 * The life cycle of a FailurePolicy mimics the life cycle of the
 * {@link DatasetSink#writer}:
 *
 * 1. When a new writer is created, the policy will be instantiated.
 * 2. As Event failures happen,
 *    {@link #handle(org.apache.flume.Event, java.lang.Throwable)} will be
 *    called to let the policy handle the failure.
 * 3. If the {@link DatasetSink} is configured to commit on batch, then the
 *    {@link #sync()} method will be called when the batch is committed.
 * 4. When the writer is closed, the policy's {@link #close()} method will be
 *    called.
 */
public interface FailurePolicy {

  /**
   * Handle a non-recoverable event.
   *
   * @param event The event
   * @param cause The cause of the failure
   * @throws EventDeliveryException The policy failed to handle the event. When
   *                                this is thrown, the Flume transaction will
   *                                be rolled back and the event will be retried
   *                                along with the rest of the batch.
   */
  public void handle(Event event, Throwable cause)
      throws EventDeliveryException;

  /**
   * Ensure any handled events are on stable storage.
   *
   * This allows the policy implementation to sync any data that it may not
   * have fully handled.
   *
   * See {@link Syncable#sync()}.
   *
   * @throws EventDeliveryException The policy failed while syncing data.
   *                                When this is thrown, the Flume transaction
   *                                will be rolled back and the batch will be
   *                                retried.
   */
  public void sync() throws EventDeliveryException;

  /**
   * Close this FailurePolicy and release any resources.
   *
   * @throws EventDeliveryException The policy failed while closing resources.
   *                                When this is thrown, the Flume transaction
   *                                will be rolled back and the batch will be
   *                                retried.
   */
  public void close() throws EventDeliveryException;

  /**
   * Knows how to build {@code FailurePolicy}s. Implementers must provide a
   * no-arg constructor.
   */
  public static interface Builder {

    /**
     * Build a new {@code FailurePolicy}
     *
     * @param config The Flume configuration context
     * @return The {@code FailurePolicy}
     */
    FailurePolicy build(Context config);
  }

}
