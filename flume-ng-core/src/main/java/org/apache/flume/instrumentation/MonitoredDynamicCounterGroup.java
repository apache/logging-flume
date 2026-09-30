/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.flume.instrumentation.MonitoredCounterGroup.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MonitoredDynamicCounterGroup implements DynamicMBean {

  private static final Logger logger =
      LoggerFactory.getLogger(MonitoredDynamicCounterGroup.class);

  //Key for component's start time in MonitoredCounterGroup.counterMap
  private static final String COUNTER_GROUP_START_TIME = "start.time";

  // key for component's stop time in MonitoredCounterGroup.counterMap
  private static final String COUNTER_GROUP_STOP_TIME = "stop.time";

  private final Type type;
  private final String name;
  private final ConcurrentMap<String, AtomicLong> counterMap;

  private final AtomicLong startTime;
  private final AtomicLong stopTime;
  private volatile boolean registered = false;

  protected MonitoredDynamicCounterGroup(Type type, String name) {
    this.type = type;
    this.name = name;

    counterMap = new ConcurrentHashMap<String, AtomicLong>();
    startTime = new AtomicLong(0L);
    stopTime = new AtomicLong(0L);
  }

  private void unregister() {
    if (registered) {
      try {
        ObjectName objName = new ObjectName("org.apache.flume."
            + type.name().toLowerCase(Locale.ENGLISH) + ":type=" + this.name);

        if (ManagementFactory.getPlatformMBeanServer().isRegistered(objName)) {
          logger.debug("Monitored counter group for type: {}, name: {}:"
              + " Another MBean is already registered with this name. "
              + "Unregistering that pre-existing MBean now...", type, name);
          ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
          logger.debug("Monitored counter group for type: {}, name: {}:"
              + " Successfully unregistered pre-existing MBean.", type, name);
        }
      } catch (Exception ex) {
        logger.error("Exception during unregister", ex);
        logger.error("Failed to unregister monitored counter group for type: {},"
            + " name: {}", type, name);
      }
    }
  }

  private void register() {
    if (!registered) {
      try {
        ObjectName objName = new ObjectName("org.apache.flume."
            + type.name().toLowerCase(Locale.ENGLISH) + ":type=" + this.name);

        if (ManagementFactory.getPlatformMBeanServer().isRegistered(objName)) {
          logger.debug("Monitored counter group for type: {}, name: {}:"
              + " Another MBean is already registered with this name. "
              + "Unregistering that pre-existing MBean now...", type, name);
          ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
          logger.debug("Monitored counter group for type: {}, name: {}:"
              + " Successfully unregistered pre-existing MBean.", type, name);
        }
        ManagementFactory.getPlatformMBeanServer().registerMBean(this, objName);
        logger.info("Monitored counter group for type: {}, name: {}:"
            + " Successfully registered new MBean.", type, name);
        registered = true;
      } catch (Exception ex) {
        logger.error("Exception during register ", ex);
        logger.error("Failed to register monitored counter group for type: {},"
            + " name: {}", type, name);
      }
    }
  }

  public void start() {
    register();
    stopTime.set(0L);
    for (Entry<String, AtomicLong> counter : counterMap.entrySet()) {
      counter.setValue(new AtomicLong(0));
    }
    startTime.set(System.currentTimeMillis());
    logger.info("Component type: {}, name: {} started", type, name);
  }

  public void stop() {
    // Sets the stopTime for the component as the current time in milliseconds
    stopTime.set(System.currentTimeMillis());

    // Prints out a message indicating that this component has been stopped
    logger.info("Component type: {}, name: {} stopped", type, name);

    // Retrieve the type for this counter group
    final String typePrefix = type.name().toLowerCase(Locale.ENGLISH);

    // Print out the startTime for this component
    logger.info("Shutdown Metric for type: {}, name: {}.{}.{} == {}",
        new Object[]{type, name, typePrefix,
            COUNTER_GROUP_START_TIME, startTime});

    // Print out the stopTime for this component
    logger.info("Shutdown Metric for type: {}, name: {}.{}.{} == {}",
        new Object[]{type, name, typePrefix,
            COUNTER_GROUP_STOP_TIME, stopTime});

    // Retrieve and sort counter group map keys
    final List<String> mapKeys = new ArrayList<String>(counterMap.keySet());

    Collections.sort(mapKeys);

    // Cycle through and print out all the key value pairs in counterMap
    for (final String counterMapKey : mapKeys) {

      // Retrieves the value from the original counterMap.
      final long counterMapValue = get(counterMapKey);

      logger.info("Shutdown Metric for type: {}, name: {}.{} == {}",
          new Object[]{type, name, counterMapKey, counterMapValue});
    }
    unregister();
  }

  /**
   * Retrieves the current value for this key
   *
   * @param counter The key for this metric
   * @return The current value for this key
   */
  protected long get(String counter) {
    AtomicLong l = counterMap.get(counter);
    return (l == null) ? 0L : l.get();
  }

  /**
   * Sets the value for this key to the given value
   *
   * @param counter The key for this metric
   * @param value The new value for this key
   */
  protected void set(String counter, long value) {
    AtomicLong l = counterMap.get(counter);
    if (l == null) {
      AtomicLong existing =
          counterMap.putIfAbsent(counter, new AtomicLong(0));
      l = (existing != null) ? existing : counterMap.get(counter);
    }
    l.set(value);
  }

  /**
   * Atomically adds the delta to the current value for this key
   *
   * @param counter The key for this metric
   * @param delta
   * @return The updated value for this key
   */
  protected long addAndGet(String counter, long delta) {
    AtomicLong l = counterMap.get(counter);
    if (l == null) {
      AtomicLong existing =
          counterMap.putIfAbsent(counter, new AtomicLong(0));
      l = (existing != null) ? existing : counterMap.get(counter);
    }
    return l.addAndGet(delta);
  }

  /**
   * Atomically increments the current value for this key by one
   *
   * @param counter The key for this metric
   * @return The updated value for this key
   */
  protected long increment(String counter) {
    AtomicLong l = counterMap.get(counter);
    if (l == null) {
      AtomicLong existing =
          counterMap.putIfAbsent(counter, new AtomicLong(0));
      l = (existing != null) ? existing : counterMap.get(counter);
    }
    return l.incrementAndGet();
  }

  public Long getAttribute(String name)
      throws AttributeNotFoundException {
    AtomicLong value = counterMap.get(name);
    if (value != null) {
      return value.longValue();
    } else {
      throw new AttributeNotFoundException("No such property: " + name);
    }
  }

  public void setAttribute(Attribute attribute)
      throws InvalidAttributeValueException, MBeanException,
      AttributeNotFoundException {

    String name = attribute.getName();
    AtomicLong value = (AtomicLong) attribute.getValue();

    if (!(value instanceof AtomicLong)) {
      throw new InvalidAttributeValueException(
          "Attribute value not a Long: " + value);
    }
    counterMap.put(name, value);
  }

  public void setAttribute(String name, String value) {
    Attribute attribute = new Attribute(name, value);
    AttributeList list = new AttributeList();
    list.add(attribute);
    setAttributes(list);
  }

  public AttributeList getAttributes(String[] names) {
    AttributeList list = new AttributeList();
    for (String name : names) {
      AtomicLong value = counterMap.get(name);
      if (value != null) {
        list.add(new Attribute(name, value.longValue()));
      }
    }
    return list;
  }

  public AttributeList setAttributes(AttributeList list) {
    Attribute[] attrs = (Attribute[]) list.toArray(new Attribute[0]);
    AttributeList retlist = new AttributeList();
    for (Attribute attr : attrs) {
      String name = attr.getName();
      AtomicLong value = (AtomicLong) attr.getValue();
      if (value instanceof AtomicLong) {
        counterMap.put(name, value);
        retlist.add(new Attribute(name, value));
      }
    }
    return retlist;
  }

  public Object invoke(String name, Object[] args, String[] sig)
      throws MBeanException, ReflectionException {
    if ((name.equals("reload")) && ((args == null) || (args.length == 0))
        && ((sig == null) || (sig.length == 0))) {
      return null;
    }
    throw new ReflectionException(new NoSuchMethodException(name));
  }

  public MBeanInfo getMBeanInfo() {
    MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[counterMap.size()];
    int i = 0;
    for (Entry<String, AtomicLong> name : counterMap.entrySet()) {
      attrs[i] = new MBeanAttributeInfo(name.getKey(), "java.lang.String",
          "Property " + name.getKey(), true, true, false);
      i++;
    }
    return new MBeanInfo(getClass().getName(), "Property Manager MBean",
        attrs, null, null, null);
  }
}
