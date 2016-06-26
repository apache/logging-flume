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
package org.apache.flume.tools;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DirectMemoryUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(DirectMemoryUtils.class);
  private static final String MAX_DIRECT_MEMORY_PARAM =
      "-XX:MaxDirectMemorySize=";
  private static final long DEFAULT_SIZE = getDefaultDirectMemorySize();
  private static final AtomicInteger allocated = new AtomicInteger(0);

  public static ByteBuffer allocate(int size) {
    Preconditions.checkArgument(size > 0, "Size must be greater than zero");
    long maxDirectMemory = getDirectMemorySize();
    long allocatedCurrently = allocated.get();
    LOG.info("Direct Memory Allocation: " +
        " Allocation = " + size +
        ", Allocated = " + allocatedCurrently +
        ", MaxDirectMemorySize = " + maxDirectMemory +
        ", Remaining = " + Math.max(0,(maxDirectMemory - allocatedCurrently)));
    try {
      ByteBuffer result = ByteBuffer.allocateDirect(size);
      allocated.addAndGet(size);
      return result;
    } catch (OutOfMemoryError error) {
      LOG.error("Error allocating " + size + ", you likely want" +
          " to increase " + MAX_DIRECT_MEMORY_PARAM, error);
      throw error;
    }
  }
  public static void clean(ByteBuffer buffer) throws Exception {
    Preconditions.checkArgument(buffer.isDirect(),
        "buffer isn't direct!");
    Method cleanerMethod = buffer.getClass().getMethod("cleaner");
    cleanerMethod.setAccessible(true);
    Object cleaner = cleanerMethod.invoke(buffer);
    Method cleanMethod = cleaner.getClass().getMethod("clean");
    cleanMethod.setAccessible(true);
    cleanMethod.invoke(cleaner);
    allocated.getAndAdd(-buffer.capacity());
    long maxDirectMemory = getDirectMemorySize();
    LOG.info("Direct Memory Deallocation: " +
        ", Allocated = " + allocated.get() +
        ", MaxDirectMemorySize = " + maxDirectMemory +
        ", Remaining = " + Math.max(0, (maxDirectMemory - allocated.get())));

  }
  public static long getDirectMemorySize() {
    RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = Lists.reverse(RuntimemxBean.getInputArguments());
    long multiplier = 1; //for the byte case.
    for (String s : arguments) {
      if (s.contains(MAX_DIRECT_MEMORY_PARAM)) {
        String memSize = s.toLowerCase(Locale.ENGLISH)
            .replace(MAX_DIRECT_MEMORY_PARAM.toLowerCase(Locale.ENGLISH), "").trim();

        if (memSize.contains("k")) {
          multiplier = 1024;
        } else if (memSize.contains("m")) {
          multiplier = 1048576;
        } else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");
        long retValue = Long.parseLong(memSize);
        return retValue * multiplier;
      }
    }
    return DEFAULT_SIZE;
  }
  private static long getDefaultDirectMemorySize() {
    try {
      Class<?> VM = Class.forName("sun.misc.VM");
      Method maxDirectMemory = VM.getDeclaredMethod("maxDirectMemory", (Class<?>)null);
      Object result = maxDirectMemory.invoke(null, (Object[])null);
      if (result != null && result instanceof Long) {
        return (Long)result;
      }
    } catch (Exception e) {
      LOG.info("Unable to get maxDirectMemory from VM: " +
          e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    // default according to VM.maxDirectMemory()
    return Runtime.getRuntime().maxMemory();
  }
}
