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

import java.util.Collections;
import java.util.List;

import com.cloudera.util.consistenthash.ConsistentHash;

/**
 * This is uses a consistent hash mechanism to generate failover chains for a
 * particular agent.
 */
public class ConsistentHashFailoverChainManager extends FailoverChainManager {

  int failovers;

  public ConsistentHashFailoverChainManager(int failovers) {
    this.failovers = failovers;
  }

  ConsistentHash<String> bins = new ConsistentHash<String>(150, Collections
      .<String> emptyList());

  @Override
  public void addCollector(String collector) {
    bins.addBin(collector);
  }

  @Override
  public void removeCollector(String collector) {
    bins.removeBin(collector);
  }

  @Override
  public List<String> getFailovers(String agent) {
    return bins.getNUniqueBinsFor(agent, failovers);
  }

}
