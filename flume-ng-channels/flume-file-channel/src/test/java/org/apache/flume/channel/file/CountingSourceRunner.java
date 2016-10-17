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
package org.apache.flume.channel.file;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ReplicatingChannelSelector;

import com.google.common.collect.Lists;

public class CountingSourceRunner extends Thread {
  private int count;
  private final int until;
  private final PollableSource source;
  private volatile boolean run;
  private final List<Exception> errors = Lists.newArrayList();

  public CountingSourceRunner(PollableSource source) {
    this(source, Integer.MAX_VALUE);
  }

  public CountingSourceRunner(PollableSource source, int until) {
    this(source, until, null);
  }

  public CountingSourceRunner(PollableSource source, Channel channel) {
    this(source, Integer.MAX_VALUE, channel);
  }

  public CountingSourceRunner(PollableSource source, int until, Channel channel) {
    this.source = source;
    this.until = until;
    if (channel != null) {
      ReplicatingChannelSelector selector = new ReplicatingChannelSelector();
      List<Channel> channels = Lists.newArrayList();
      channels.add(channel);
      selector.setChannels(channels);
      this.source.setChannelProcessor(new ChannelProcessor(selector));
    }
  }

  @Override
  public void run() {
    run = true;
    while (run && count < until) {
      boolean error = true;
      try {
        if (PollableSource.Status.READY.equals(source.process())) {
          count++;
          error = false;
        }
      } catch (Exception ex) {
        errors.add(ex);
      }
      if (error) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  public void shutdown() {
    run = false;
  }

  public int getCount() {
    return count;
  }

  public List<Exception> getErrors() {
    return errors;
  }
}
