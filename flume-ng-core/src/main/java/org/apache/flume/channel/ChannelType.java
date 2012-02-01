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
package org.apache.flume.channel;

/**
 * Enumeration of built in channel types available in the system.
 */
public enum ChannelType {

  /**
   * Place holder for custom channels not part of this enumeration.
   */
  OTHER(null),

  /**
   * Memory channel
   * @see MemoryChannel
   */
  MEMORY(MemoryChannel.class.getName()),

  /**
   * JDBC channel provided by org.apache.flume.channel.jdbc.JdbcChannel
   */
  JDBC("org.apache.flume.channel.jdbc.JdbcChannel");

  private final String channelClassName;

  private ChannelType(String channelClassName) {
    this.channelClassName = channelClassName;
  }

  public String getChannelClassName() {
    return channelClassName;
  }
}
