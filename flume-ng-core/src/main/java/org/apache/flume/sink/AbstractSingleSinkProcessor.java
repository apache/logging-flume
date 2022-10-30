/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.sink;

import java.util.List;

import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.lifecycle.LifecycleState;

import com.google.common.base.Preconditions;

/**
 * A Sink Processor that only accesses a single Sink.
 */
public abstract class AbstractSingleSinkProcessor implements SinkProcessor {
  protected Sink sink;
  private LifecycleState lifecycleState;

  @Override
  public void start() {
    Preconditions.checkNotNull(sink, "DefaultSinkProcessor sink not set");
    sink.start();
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {
    Preconditions.checkNotNull(sink, "DefaultSinkProcessor sink not set");
    sink.stop();
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  @Override
  public void setSinks(List<Sink> sinks) {
    Preconditions.checkNotNull(sinks);
    Preconditions.checkArgument(sinks.size() == 1, "DefaultSinkPolicy can "
        + "only handle one sink, "
        + "try using a policy that supports multiple sinks");
    sink = sinks.get(0);
  }

  public Sink getSink() {
    return sink;
  }
}
