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

package org.apache.flume;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Used for counting events, collecting metrics, etc.
 */
public class CounterGroup {

  private String name;
  private volatile ConcurrentHashMap<String, LongAdder> counters;

  public CounterGroup() {
    counters = new ConcurrentHashMap<>();
  }

  public Long get(String name) {
    return getCounter(name).sum();
  }

  public Long incrementAndGet(String name) {
    LongAdder counter = getCounter(name);
    counter.increment();
    return counter.sum();
  }

  public Long addAndGet(String name, Long delta) {
    LongAdder counter = getCounter(name);
    counter.add(delta);
    return counter.sum();
  }

  public void add(CounterGroup counterGroup) {
    for (Map.Entry<String, LongAdder> entry : counterGroup.getCounters().entrySet()) {
      addAndGet(entry.getKey(), entry.getValue().sum());
    }
  }

  public void set(String name, Long value) {
    LongAdder counter = getCounter(name);
    counter.reset();
    counter.add(value);
  }

  public LongAdder getCounter(String name) {
    if (!counters.containsKey(name)) {
      counters.put(name, new LongAdder());
    }
    return counters.get(name);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ConcurrentHashMap<String, LongAdder> getCounters() {
    return counters;
  }

  public void setCounters(ConcurrentHashMap<String, LongAdder> counters) {
    this.counters = counters;
  }

  @Override
  public String toString() {
    return "{ name:" + name + " counters:" + counters + " }";
  }
}
