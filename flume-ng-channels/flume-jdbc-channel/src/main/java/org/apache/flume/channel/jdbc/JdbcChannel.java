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

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.channel.AbstractChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * <p>
 * A JDBC based channel implementation.
 * </p>
 * <p>
 * JdbcChannel is marked
 * {@link org.apache.flume.annotations.InterfaceAudience.Private} because it
 * should only be instantiated via a configuration. For example, users should
 * certainly use JdbcChannel but not by instantiating JdbcChannel objects.
 * Meaning the label Private applies to user-developers not user-operators.
 * In cases where a Channel is required by instantiated by user-developers
 * {@link org.apache.flume.channel.MemoryChannel} should be used.
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class JdbcChannel extends AbstractChannel {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcChannel.class);

  private JdbcChannelProvider provider;

  /**
   * Instantiates a new JDBC Channel.
   */
  public JdbcChannel() {
  }

  @Override
  public void put(Event event) throws ChannelException {
    getProvider().persistEvent(getName(), event);
  }

  @Override
  public Event take() throws ChannelException {
    return getProvider().removeEvent(getName());
  }

  @Override
  public Transaction getTransaction() {
    return getProvider().getTransaction();
  }

  @Override
  public void stop() {
    JdbcChannelProviderFactory.releaseProvider(getName());
    provider = null;

    super.stop();
  }

  private JdbcChannelProvider getProvider() {
    return provider;
  }

  @Override
  public void configure(Context context) {
    provider = JdbcChannelProviderFactory.getProvider(context, getName());

    LOG.info("JDBC Channel initialized: " + getName());
  }
}
