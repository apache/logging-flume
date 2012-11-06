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
package org.apache.flume;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.lifecycle.LifecycleAware;

/**
 * <p>
 * A channel connects a {@link Source} to a {@link Sink}. The source
 * acts as producer while the sink acts as a consumer of events. The channel
 * itself is the buffer between the two.
 * </p>
 * <p>
 * A channel exposes a {@link Transaction} interface that can be used by
 * its clients to ensure atomic {@linkplain #put(Event) put} and
 * {@linkplain #take() take} semantics.
 * This is necessary to guarantee single hop reliability between agents.
 * For instance, a source will successfully produce an {@linkplain Event event}
 * if and only if that event can be committed to the source's associated
 * channel. Similarly, a sink will consume an event if and
 * only if its respective endpoint can accept the event. The
 * extent of transaction support varies for different channel implementations
 * ranging from strong to best-effort semantics.
 * </p>
 * <p>
 * Channels are associated with unique {@linkplain NamedComponent names} that
 * can be used for separating configuration and working namespaces.
 * </p>
 * <p>
 * Channels must be thread safe, protecting any internal invariants as no
 * guarantees are given as to when and by how many sources/sinks they may
 * be simultaneously accessed by.
 * </p>
 *
 * @see org.apache.flume.Source
 * @see org.apache.flume.Sink
 * @see org.apache.flume.Transaction
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Channel extends LifecycleAware, NamedComponent {

  /**
   * <p>Puts the given event into the channel.</p>
   * <p><strong>Note</strong>: This method must be invoked within an active
   * {@link Transaction} boundary. Failure to do so can lead to unpredictable
   * results.</p>
   * @param event the event to transport.
   * @throws ChannelException in case this operation fails.
   * @see org.apache.flume.Transaction#begin()
   */
  public void put(Event event) throws ChannelException;

  /**
   * <p>Returns the next event from the channel if available. If the channel
   * does not have any events available, this method must return {@code null}.
   * </p>
   * <p><strong>Note</strong>: This method must be invoked within an active
   * {@link Transaction} boundary. Failure to do so can lead to unpredictable
   * results.</p>
   * @return the next available event or {@code null} if no events are
   * available.
   * @throws ChannelException in case this operation fails.
   * @see org.apache.flume.Transaction#begin()
   */
  public Event take() throws ChannelException;

  /**
   * @return the transaction instance associated with this channel.
   */
  public Transaction getTransaction();
}
