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
package org.apache.flume.channel.jdbc;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;

/**
 * Service provider interface for JDBC channel providers.
 */
public interface JdbcChannelProvider {

  /**
   * Initializes the channel provider. This method must be called before
   * the channel can be used in any way.
   * @param properties the configuration for the system
   */
  public void initialize(Context context);

  /**
   * Deinitializes the channel provider. Once this method is called, the
   * channel provider cannot be used and must be discarded.
   */
  public void close();

  /**
   * Writes the event to the persistent store.
   * @param channelName
   * @param event
   */
  public void persistEvent(String channelName, Event event);


  /**
   * Removes the next event for the named channel from the underlying
   * persistent store.
   * @param channelName
   * @return
   */
  public Event removeEvent(String channelName);

  /**
   * @return the transaction associated with the current thread.
   */
  public Transaction getTransaction();

}
