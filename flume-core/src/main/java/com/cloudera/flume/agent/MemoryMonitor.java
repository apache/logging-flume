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
package com.cloudera.flume.agent;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

/**
 * This memory warning system will call the listeners when we exceed the
 * percentage of available memory specified. This is a singleton class.
 * 
 * Based on code from: http://www.javaspecialists.co.za/archive/Issue092.html
 */
public class MemoryMonitor {
  static MemoryMonitor singleton = new MemoryMonitor();

  private final Collection<Listener> listeners = Collections
      .synchronizedList(new ArrayList<Listener>());

  public interface Listener {
    public void memoryUsageLow(long usedMemory, long maxMemory);
  }

  private MemoryMonitor() {
    final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    NotificationEmitter emitter = (NotificationEmitter) mbean;
    emitter.addNotificationListener(new NotificationListener() {
      public void handleNotification(Notification n, Object hb) {
        if (n.getType()
            .equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
          long maxMemory = tenuredGenPool.getUsage().getMax();
          long usedMemory = tenuredGenPool.getUsage().getUsed();

          for (Listener listener : listeners) {
            listener.memoryUsageLow(usedMemory, maxMemory);
          }
        }
      }
    }, null, null);
  }

  public static MemoryMonitor getMemoryMonitor() {
    return singleton;
  }

  public boolean addListener(Listener listener) {
    return listeners.add(listener);
  }

  public boolean removeListener(Listener listener) {
    return listeners.remove(listener);
  }

  private static final MemoryPoolMXBean tenuredGenPool = findTenuredGenPool();

  public static void setPercentageUsageThreshold(double percentage) {
    if (percentage <= 0.0 || percentage > 1.0) {
      throw new IllegalArgumentException("Percentage not in range");
    }
    long maxMemory = tenuredGenPool.getUsage().getMax();
    long warningThreshold = (long) (maxMemory * percentage);
    tenuredGenPool.setUsageThreshold(warningThreshold);
  }

  public long getMemUsage() {
    return tenuredGenPool.getUsage().getUsed();
  }

  public long getMemMax() {
    return tenuredGenPool.getUsage().getMax();
  }

  /**
   * Tenured Space Pool can be determined by it being of type HEAP and by it
   * being possible to set the usage threshold.
   */
  private static MemoryPoolMXBean findTenuredGenPool() {
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      // I don't know whether this approach is better, or whether
      // we should rather check for the pool name "Tenured Gen"?
      if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
        return pool;
      }
    }
    throw new AssertionError("Could not find tenured space");
  }

  /**
   * This sets up a trigger that hard exit's when a trigger happens and gc
   * doesn't relieve memory pressure
   * 
   * @param threshold
   */
  public static void setupHardExitMemMonitor(final double threshold) {
    MemoryMonitor.setPercentageUsageThreshold(threshold);
    final MemoryMonitor mem = MemoryMonitor.getMemoryMonitor();
    Listener l = new Listener() {
      @Override
      public void memoryUsageLow(long usedMemory, long maxMemory) {
        System.gc();

        // are we still using too much memory?
        long umem = mem.getMemUsage();
        long mmem = mem.getMemMax();
        double percent = (umem) / (double)mmem;

        if (percent > threshold) {
          System.err
              .printf(
                  "%dMB/%dMB memory used (%.1f%%)\nExiting due to imminent OutOfMemoryError!\n",
                  usedMemory / 1024 / 1024, maxMemory / 1024 / 1024,
                  percent * 100.0);
          System.exit(-1);
        }
      }

    };
    mem.addListener(l);
  }
}
