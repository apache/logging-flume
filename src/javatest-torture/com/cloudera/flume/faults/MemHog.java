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
package com.cloudera.flume.faults;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * This allocates and holds valid references to the memory to exhause heap space. 
 */
public class MemHog extends ResourceHog {

  static Logger logger = Logger.getLogger(MemHog.class.getName());

  int delay;
  boolean random;
  int increment;

  MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

  List<byte[]> memHog = new ArrayList<byte[]>();

  public MemHog() {
    this(20 * 1024 * 1024, 1000, true);
  }

  public MemHog(int increment, int delay, boolean random) {
    super(delay, random);
    this.increment = increment;
  }

  @Override
  public void increment() {
    memHog.add(new byte[increment]);

    logger.info("Using " + mem.getHeapMemoryUsage().getUsed() / 1024 / 1024
        + "MB of memory");
  }

}
