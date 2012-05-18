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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FailoverSinkProcessor maintains a prioritized list of sinks,
 * guarranteeing that so long as one is available events will be processed.
 *
 * The failover mechanism works by relegating failed sinks to a pool
 * where they are assigned a cooldown period, increasing with sequential
 * failures before they are retried. Once a sink succesfully sends an
 * event it is restored to the live pool.
 *
 * FailoverSinkProcessor is in no way thread safe and expects to be run via
 * SinkRunner Additionally, setSinks must be called before configure, and
 * additional sinks cannot be added while running
 *
 * To configure, set a sink groups processor to "failover" and set priorities
 * for individual sinks, all priorities must be unique. Furthermore, an
 * upper limit to failover time can be set(in miliseconds) using maxpenalty
 *
 * Ex)
 *
 * host1.sinkgroups = group1
 *
 * host1.sinkgroups.group1.sinks = sink1 sink2
 * host1.sinkgroups.group1.processor.type = failover
 * host1.sinkgroups.group1.processor.priority.sink1 = 5
 * host1.sinkgroups.group1.processor.priority.sink2 = 10
 * host1.sinkgroups.group1.processor.maxpenalty = 10000
 *
 */
public class FailoverSinkProcessor extends AbstractSinkProcessor {
  private static final int FAILURE_PENALTY = 1000;
  private static final int DEFAULT_MAX_PENALTY = 30000;

  private class FailedSink implements Comparable<FailedSink> {
    private Long refresh;
    private Integer priority;
    private Sink sink;
    private Integer sequentialFailures;
    public FailedSink(Integer priority, Sink sink, int seqFailures) {
      this.sink = sink;
      this.priority = priority;
      this.sequentialFailures = seqFailures;
      adjustRefresh();
    }
    @Override
    public int compareTo(FailedSink arg0) {
      return refresh.compareTo(arg0.refresh);
    }

    public Long getRefresh() {
      return refresh;
    }

    public Sink getSink() {
      return sink;
    }
    public Integer getPriority() {
      return priority;
    }
    public void incFails() {
      sequentialFailures++;
      adjustRefresh();
      logger.debug("Sink {} failed again, new refresh is at {}, " +
            "current time {}", new Object[] {
              sink.getName(), refresh, System.currentTimeMillis()});
    }
    private void adjustRefresh() {
      refresh = System.currentTimeMillis()
              + Math.min(maxPenalty, (1 << sequentialFailures) * FAILURE_PENALTY);
    }
  }

  private static final Logger logger = LoggerFactory
      .getLogger(FailoverSinkProcessor.class);

  private static final String PRIORITY_PREFIX = "priority.";
  private static final String MAX_PENALTY_PREFIX = "maxpenalty";
  private Map<String, Sink> sinks;
  private Sink activeSink;
  private SortedMap<Integer, Sink> liveSinks;
  private Queue<FailedSink> failedSinks;
  private int maxPenalty;

  @Override
  public void configure(Context context) {
    liveSinks = new TreeMap<Integer, Sink>();
    failedSinks = new PriorityQueue<FailedSink>();
    Integer nextPrio = 0;
    String maxPenaltyStr = context.getString(MAX_PENALTY_PREFIX);
    if(maxPenaltyStr == null) {
      maxPenalty = DEFAULT_MAX_PENALTY;
    } else {
      try {
        maxPenalty = Integer.parseInt(maxPenaltyStr);
      } catch (NumberFormatException e) {
        logger.warn("{} is not a valid value for {}",
                new Object[] { maxPenaltyStr, MAX_PENALTY_PREFIX });
        maxPenalty  = DEFAULT_MAX_PENALTY;
      }
    }
    for (Entry<String, Sink> entry : sinks.entrySet()) {
      String priStr = PRIORITY_PREFIX + entry.getKey();
      Integer priority;
      try {
        priority =  Integer.parseInt(context.getString(priStr));
      } catch (Exception e) {
        priority = --nextPrio;
      }
      if(!liveSinks.containsKey(priority)) {
        liveSinks.put(priority, sinks.get(entry.getKey()));
      } else {
        logger.warn("Sink {} not added to FailverSinkProcessor as priority" +
            "duplicates that of sink {}", entry.getKey(),
            liveSinks.get(priority));
      }
    }
    activeSink = liveSinks.get(liveSinks.lastKey());
  }

  @Override
  public Status process() throws EventDeliveryException {
    // Retry any failed sinks that have gone through their "cooldown" period
    Long now = System.currentTimeMillis();
    while(!failedSinks.isEmpty() && failedSinks.peek().getRefresh() < now) {
      FailedSink cur = failedSinks.poll();
      Status s;
      try {
        s = cur.getSink().process();
        if (s  == Status.READY) {
          liveSinks.put(cur.getPriority(), cur.getSink());
          activeSink = liveSinks.get(liveSinks.lastKey());
          logger.debug("Sink {} was recovered from the fail list",
                  cur.getSink().getName());
        } else {
          // if it's a backoff it needn't be penalized.
          failedSinks.add(cur);
        }
        return s;
      } catch (Exception e) {
        cur.incFails();
        failedSinks.add(cur);
      }
    }

    Status ret = null;
    while(activeSink != null) {
      try {
        ret = activeSink.process();
        return ret;
      } catch (Exception e) {
        logger.warn("Sink {} failed and has been sent to failover list",
                activeSink.getName(), e);
        activeSink = moveActiveToDeadAndGetNext();
      }
    }

    throw new EventDeliveryException("All sinks failed to process, " +
        "nothing left to failover to");
  }

  private Sink moveActiveToDeadAndGetNext() {
    Integer key = liveSinks.lastKey();
    failedSinks.add(new FailedSink(key, activeSink, 1));
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
    // needed to implement the start/stop functionality
    super.setSinks(sinks);

    this.sinks = new HashMap<String, Sink>();
    for (Sink sink : sinks) {
      this.sinks.put(sink.getName(), sink);
    }
  }

}
