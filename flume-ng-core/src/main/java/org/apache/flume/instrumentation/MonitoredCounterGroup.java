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
package org.apache.flume.instrumentation;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MonitoredCounterGroup {

  private static final Logger LOG =
      LoggerFactory.getLogger(MonitoredCounterGroup.class);

  private final Type type;
  private final String name;
  private final Map<String, AtomicLong> counterMap;

  private AtomicLong startTime;
  private AtomicLong stopTime;
  private volatile boolean registered = false;


  protected MonitoredCounterGroup(Type type, String name, String... attrs) {
    this.type = type;
    this.name = name;

    Map<String, AtomicLong> counterInitMap = new HashMap<String, AtomicLong>();

    // Initialize the counters
    for (String attribute : attrs) {
      counterInitMap.put(attribute, new AtomicLong(0L));
    }

    counterMap = Collections.unmodifiableMap(counterInitMap);

    startTime = new AtomicLong(0L);
    stopTime = new AtomicLong(0L);

  }

  public void start() {

    register();
    stopTime.set(0L);
    for (String counter : counterMap.keySet()) {
      counterMap.get(counter).set(0L);
    }
    startTime.set(System.currentTimeMillis());
    LOG.info("Component type: " + type + ", name: " + name + " started");
  }

  /**
   * Registers the counter. This method should be used only for testing, and
   * there should be no need for any implementations to directly call this
   * method.
   */
  void register() {
    if (!registered) {
      try {
        ObjectName objName = new ObjectName("org.apache.flume."
                + type.name().toLowerCase() + ":type=" + this.name);

        ManagementFactory.getPlatformMBeanServer().registerMBean(this, objName);
        registered = true;
        LOG.info("Monitoried counter group for type: " + type + ", name: " + name
                + ", registered successfully.");
      } catch (Exception ex) {
        LOG.error("Failed to register monitored counter group for type: "
                + type + ", name: " + name, ex);
      }
    }
  }

  public void stop() {
    stopTime.set(System.currentTimeMillis());
    LOG.info("Component type: " + type + ", name: " + name + " stopped");
  }

  public long getStartTime() {
    return startTime.get();
  }

  public long getStopTime() {
    return stopTime.get();
  }

  @Override
  public final String toString() {
    StringBuilder sb = new StringBuilder(type.name()).append(":");
    sb.append(name).append("{");
    boolean first = true;
    Iterator<String> counterIterator = counterMap.keySet().iterator();
    while (counterIterator.hasNext()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      String counterName = counterIterator.next();
      sb.append(counterName).append("=").append(get(counterName));
    }
    sb.append("}");

    return sb.toString();
  }


  protected long get(String counter) {
    return counterMap.get(counter).get();
  }

  protected void set(String counter, long value) {
    counterMap.get(counter).set(value);
  }

  protected long addAndGet(String counter, long delta) {
    return counterMap.get(counter).addAndGet(delta);
  }

  protected long increment(String counter) {
    return counterMap.get(counter).incrementAndGet();
  }

  public static enum Type {
    SOURCE,
    CHANNEL_PROCESSOR,
    CHANNEL,
    SINK_PROCESSOR,
    SINK,
    INTERCEPTOR,
    SERIALIZER,
    OTHER
  };

  public String getType(){
    return type.name();
  }
}
