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

/**
 * <p>
 * A channel connects a <tt>Source</tt> to a <tt>Sink</tt>. The source
 * acts as producer while the sink acts as a consumer of events. The channel
 * itself is the buffer between the two.
 * </p>
 * <p>
 * A channel exposes a <tt>Transaction</tt> interface that can be used by
 * its clients to ensure atomic <tt>put</tt> or <tt>remove</tt>
 * semantics. This is necessary to guarantee single hop reliability in a
 * logical node. For instance, a source will produce an event successfully
 * if and only if it can be committed to the channel. Similarly, a sink will
 * consume an event if and only if its end point can accept the event. The
 * extent of transaction support varies for different channel implementations
 * ranging from strong to best-effort semantics.
 * </p>
 *
 * @see org.apache.flume.EventSource
 * @see org.apache.flume.EventSink
 * @see org.apache.flume.Transaction
 */
public interface Channel {

  /**
   * <p>Puts the given event in the channel.</p>
   * <p><strong>Note</strong>: This method must be invoked within an active
   * <tt>Transaction</tt> boundary. Failure to do so can lead to unpredictable
   * results.</p>
   * @param event the event to transport.
   * @throws ChannelException in case this operation fails.
   * @see org.apache.flume.Transaction#begin()
   */
  public void put(Event event) throws ChannelException;

  /**
   * <p>Returns the next event from the channel if available. If the channel
   * does not have any events available, this method would return <tt>null</tt>.
   * </p>
   * <p><strong>Note</strong>: This method must be invoked within an active
   * <tt>Transaction</tt> boundary. Failure to do so can lead to unpredictable
   * results.</p>
   * @return the next available event or <tt>null</tt> if no events are
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
