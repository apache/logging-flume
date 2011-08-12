package org.apache.flume.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CounterGroup {

  private String name;
  private ConcurrentHashMap<String, AtomicLong> counters;

  public CounterGroup() {
    counters = new ConcurrentHashMap<String, AtomicLong>();
  }

  public Long get(String name) {
    return getCounter(name).get();
  }

  public Long incrementAndGet(String name) {
    return getCounter(name).incrementAndGet();
  }

  public Long addAndGet(String name, Long delta) {
    return getCounter(name).addAndGet(delta);
  }

  public AtomicLong getCounter(String name) {
    synchronized (counters) {
      if (!counters.containsKey(name)) {
        counters.put(name, new AtomicLong());
      }
    }

    return counters.get(name);
  }

  @Override
  public String toString() {
    return "{ name:" + name + " counters:" + counters + " }";
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ConcurrentHashMap<String, AtomicLong> getCounters() {
    return counters;
  }

  public void setCounters(ConcurrentHashMap<String, AtomicLong> counters) {
    this.counters = counters;
  }

}
