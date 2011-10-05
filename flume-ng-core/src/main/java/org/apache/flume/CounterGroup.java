package org.apache.flume;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class CounterGroup {

  private String name;
  private HashMap<String, AtomicLong> counters;

  public CounterGroup() {
    counters = new HashMap<String, AtomicLong>();
  }

  public synchronized Long get(String name) {
    return getCounter(name).get();
  }

  public synchronized Long incrementAndGet(String name) {
    return getCounter(name).incrementAndGet();
  }

  public synchronized Long addAndGet(String name, Long delta) {
    return getCounter(name).addAndGet(delta);
  }

  public synchronized void add(CounterGroup counterGroup) {
    synchronized (counterGroup) {
      for (Entry<String, AtomicLong> entry : counterGroup.getCounters()
          .entrySet()) {

        addAndGet(entry.getKey(), entry.getValue().get());
      }
    }
  }

  public synchronized void set(String name, Long value) {
    getCounter(name).set(value);
  }

  public synchronized AtomicLong getCounter(String name) {
    if (!counters.containsKey(name)) {
      counters.put(name, new AtomicLong());
    }

    return counters.get(name);
  }

  @Override
  public synchronized String toString() {
    return "{ name:" + name + " counters:" + counters + " }";
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized void setName(String name) {
    this.name = name;
  }

  public synchronized HashMap<String, AtomicLong> getCounters() {
    return counters;
  }

  public synchronized void setCounters(HashMap<String, AtomicLong> counters) {
    this.counters = counters;
  }

}
