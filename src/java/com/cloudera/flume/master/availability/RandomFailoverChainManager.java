/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master.availability;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.util.ReservoirSampler;

/**
 * This uses a hash of the agent names a seed to randomly generate a failover
 * list.
 * 
 * TODO (jon) This is not complete because it needs to update failover chains
 * when new collectors are updated. I'm punting on this because it is handled
 * automatically by the ConsistentHash implementation.
 * 
 */
public class RandomFailoverChainManager extends FailoverChainManager {

  int failovers;

  public RandomFailoverChainManager(int failovers) {
    this.failovers = failovers;
  }

  Set<String> collectors = new HashSet<String>();
  // after some values are assigned, keep them here. When collectors change, we
  // can pick and choose from here to update configurations.
  Map<String, List<String>> assignments = new HashMap<String, List<String>>();

  @Override
  public void addCollector(String collector) {
    collectors.add(collector);

    // TODO (jon) reassign all assignments -- punting and going to consistent
    // hash.

  }

  @Override
  public void removeCollector(String collector) {
    collectors.remove(collector);

    // TODO (jon) check existing assignments, and reshuffle if contained the
    // value. Need to check all assignments

  }

  List<String> genRandomCollectors(int n, int seed) {
    // this is order n where n is number of collectors. (which is small).

    // takes n items each with equal probability chance
    ReservoirSampler<String> res = new ReservoirSampler<String>(n, seed);
    for (String col : collectors) {
      res.onNext(col);
    }
    return res.sample();
  }

  @Override
  public List<String> getFailovers(String agent) {
    List<String> colls = assignments.get(agent);
    if (colls == null) {
      // use the hashCode as a seed to make this deterministic.
      colls = genRandomCollectors(failovers, agent.hashCode());
      assignments.put(agent, colls);
    }
    return colls;
  }

}
