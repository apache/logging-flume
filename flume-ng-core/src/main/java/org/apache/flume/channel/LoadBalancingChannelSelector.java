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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Load balancing channel selector. This selector allows for load balancing
 * between channels based on various policy configuration options. This serves a similar purpose
 * to the LoadBalancingSinkProcessor except it allows the sinks to run in their own threads instead
 * of in just one.
 *
 * <p>The <tt>LoadBalancingChannelSelector</tt> maintains an indexed list of
 * active channels on which the load must be distributed. This implementation
 * supports distributing load using either via <tt>ROUND_ROBIN</tt> or via
 * <tt>RANDOM</tt> selection mechanism. The choice of selection mechanism
 * defaults to <tt>ROUND_ROBIN</tt> type, but can be overridden via
 * configuration.</p>
 */
public class LoadBalancingChannelSelector extends AbstractChannelSelector {
  private final List<Channel> emptyList = Collections.emptyList();
  private ChannelPicker picker;

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    Channel ch = picker.getChannel();
    Preconditions.checkNotNull(ch, "Channel picker returned null");
    return Lists.newArrayList(ch);
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return emptyList;
  }

  @Override
  public void configure(Context context) {
    List<Channel> channels = getAllChannels();
    String strPolicy = context.getString("policy", Policy.ROUND_ROBIN.toString());
    Policy policy;
    // instantiate policy
    try {
      policy = Policy.valueOf(strPolicy.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Invalid policy: " + strPolicy, ex);
    }
    // instantiate picker
    try {
      picker = policy.getPolicyClass().newInstance();
      picker.setChannels(channels);
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new IllegalArgumentException("Cannot instantiate policy class from policy enum "
          + policy, ex);
    }
  }

  /**
   * Definitions for the various policy types
   */
  private enum Policy {
    ROUND_ROBIN(RoundRobinPolicy.class),
    RANDOM(RandomPolicy.class);

    private final Class<? extends ChannelPicker> clazz;

    Policy(Class<? extends ChannelPicker> clazz) {
      this.clazz = clazz;
    }

    public Class<? extends ChannelPicker> getPolicyClass() {
      return clazz;
    }
  }

  private interface ChannelPicker {
    Channel getChannel();

    void setChannels(List<Channel> channels);
  }

  /**
   * Selects channels in a round-robin fashion
   */
  private static class RoundRobinPolicy implements ChannelPicker {

    private final AtomicInteger next = new AtomicInteger(0);
    private List<Channel> channels;

    public RoundRobinPolicy() {
    }

    @Override
    public void setChannels(List<Channel> channels) {
      this.channels = channels;
    }

    @Override
    public Channel getChannel() {
      return channels.get(next.getAndAccumulate(channels.size(), (x, y) -> ++x < y ? x : 0));
    }
  }

  /**
   * Selects a channel at random
   */
  private static class RandomPolicy implements ChannelPicker {
    private List<Channel> channels;
    private final Random random = new Random(System.currentTimeMillis());

    public RandomPolicy() {
    }

    @Override
    public void setChannels(List<Channel> channels) {
      this.channels = channels;
    }

    @Override
    public Channel getChannel() {
      int size = channels.size();
      int pick = random.nextInt(size);
      return channels.get(pick);
    }

  }
}
