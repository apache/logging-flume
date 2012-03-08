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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * Replicating channel selector. This selector allows the event to be placed
 * in all the channels that the source is configured with.
 */
public class ReplicatingChannelSelector extends AbstractChannelSelector {

  private final List<Channel> emptyList = Collections.emptyList();

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    return getAllChannels();
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    return emptyList;
  }

  @Override
  public void configure(Context context) {
    // No configuration necessary
  }
}
