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

package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Sink;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;

import com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Stable
abstract public class AbstractSink implements Sink, LifecycleAware {

  private Channel channel;
  private String name;

  private LifecycleState lifecycleState;

  public AbstractSink() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(channel != null, "No channel configured");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public synchronized void stop() {
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public synchronized Channel getChannel() {
    return channel;
  }

  @Override
  public synchronized void setChannel(Channel channel) {
    this.channel = channel;
  }

  @Override
  public synchronized LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  @Override
  public synchronized void setName(String name) {
    this.name = name;
  }

  @Override
  public synchronized String getName() {
    return name;
  }

  public String toString() {
	  return this.getClass().getName() + "{name:" + name + ", channel:" + channel.getName() + "}";
  }
}
