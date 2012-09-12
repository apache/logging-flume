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

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.LoadBalancingSinkProcessor.SinkSelector;

public abstract class AbstractSinkSelector implements SinkSelector {

  private LifecycleState state;

  // List of sinks as specified
  private List<Sink> sinkList;

  protected long maxTimeOut = 0;

  @Override
  public void configure(Context context) {
    Long timeOut = context.getLong("maxTimeOut");
    if(timeOut != null){
      maxTimeOut = timeOut;
    }
  }

  @Override
  public void start() {
    state = LifecycleState.START;
  }

  @Override
  public void stop() {
    state = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return state;
  }

  @Override
  public void setSinks(List<Sink> sinks) {
    sinkList = new ArrayList<Sink>();
    sinkList.addAll(sinks);
  }

  protected List<Sink> getSinks() {
    return sinkList;
  }

  @Override
  public void informSinkFailed(Sink failedSink) {
    // no-op
  }
}
