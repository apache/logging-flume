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
package org.apache.flume.sink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.lifecycle.LifecycleState;

/**
 * A convenience base class for sink processors.
 */
public abstract class AbstractSinkProcessor implements SinkProcessor {

  private LifecycleState state;

  // List of sinks as specified
  private List<Sink> sinkList;

  @Override
  public void start() {
    for(Sink s : sinkList) {
      s.start();
    }

    state = LifecycleState.START;
  }

  @Override
  public void stop() {
    for(Sink s : sinkList) {
      s.stop();
    }
    state = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return state;
  }

  @Override
  public void setSinks(List<Sink> sinks) {
    List<Sink> list = new ArrayList<Sink>();
    list.addAll(sinks);
    sinkList = Collections.unmodifiableList(list);
  }

  protected List<Sink> getSinks() {
    return sinkList;
  }
}
