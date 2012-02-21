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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.Sink.Status;
import org.apache.flume.lifecycle.LifecycleState;

/**
 * FailoverSinkProcessor is in no way thread safe and expects to be run via
 * SinkRunner Additionally, setSinks must be called before configure, and
 * additional sinks cannot be added while running
 * 
 * To configure, set a sink groups processor to "failover" and set priorities
 * for individual sinks:
 * 
 * Ex)
 * 
 * host1.sinkgroups = group1
 * 
 * host1.sinkgroups.group1.sinks = sink1 sink2
 * host1.sinkgroups.group1.processor.type = failover
 * host1.sinkgroups.group1.processor.priority.sink1 = 5
 * host1.sinkgroups.group1.processor.priority.sink2 = 10
 * 
 */
public class FailoverSinkProcessor implements SinkProcessor {

  private static final String PRIORITY_PREFIX = "priority.";
  private Map<String, Sink> sinks;
  private Sink activeSink;
  private SortedMap<Integer, Sink> liveSinks;
  private SortedMap<Integer, Sink> deadSinks;
  private LifecycleState state;

  @Override
  public void start() {
    for(Sink s : sinks.values()) {
      s.start();
    }
    state = LifecycleState.START;
  }

  @Override
  public void stop() {
    for(Sink s : sinks.values()) {
      s.stop();
    }
    state = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return state;
  }

  @Override
  public void configure(Context context) {
    liveSinks = new TreeMap<Integer, Sink>();
    deadSinks = new TreeMap<Integer, Sink>();
    Integer nextPrio = 0;
    for (Entry<String, Sink> entry : sinks.entrySet()) {
      String priStr = PRIORITY_PREFIX + entry.getKey();
      Integer priority;
      try {
        priority =  Integer.parseInt(context.getString(priStr));
      } catch (NumberFormatException e) {
        priority = --nextPrio;
      } catch (NullPointerException e) {
        priority = --nextPrio;
      }
      liveSinks.put(priority, sinks.get(entry.getKey()));
    }
    activeSink = liveSinks.get(liveSinks.lastKey());
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status ret = null;
    while(activeSink != null) {
      try {
        ret = activeSink.process();
        return ret;
      } catch (EventDeliveryException e) {
        activeSink = moveActiveToDeadAndGetNext();
      }
    }

    // if none of the live ones worked, give the dead ones a go,
    // then give up
    for (Entry<Integer, Sink> entry : deadSinks.entrySet()) {
      try {
        ret = entry.getValue().process();
        // if it worked, put it in the liveSinks pile and rejoice
        if (ret == Status.READY) {
          activeSink = entry.getValue();
          liveSinks.put(entry.getKey(), entry.getValue());
          deadSinks.remove(entry.getKey());
          return ret;
        }
      } catch (EventDeliveryException e) {

      }
    }
    throw new EventDeliveryException("All sinks failed to process, " +
        "nothing left to failover to");
  }

  private Sink moveActiveToDeadAndGetNext() {
    Integer key = liveSinks.lastKey();
    deadSinks.put(key, activeSink);
    liveSinks.remove(key);
    if(liveSinks.isEmpty()) return null;
    if(liveSinks.lastKey() != null) {
      return liveSinks.get(liveSinks.lastKey());
    } else {
      return null;
    }
  }

  @Override
  public void setSinks(List<Sink> sinks) {
    this.sinks = new HashMap<String, Sink>();
    for (Sink sink : sinks) {
      this.sinks.put(sink.getName(), sink);
    }
  }

}
