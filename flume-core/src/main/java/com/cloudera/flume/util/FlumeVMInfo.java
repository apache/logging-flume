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
package com.cloudera.flume.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.util.Date;
import java.util.Map;

import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;

/**
 * This is a quick and dirty reportable that gathers information about the
 * current Flume process.
 */
public class FlumeVMInfo implements Reportable {
  String name = "JVMInfo";

  @Override
  public String getName() {
    return name;
  }

  public FlumeVMInfo(String name) {
    this.name = name + "jvm-Info";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());

    // TODO (jon) class loader

    // TODO (jon) compilation

    // TODO (jon) gc

    // TODO (jon) mem man

    // mem
    MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = mem.getHeapMemoryUsage();
    rpt.setLongMetric("mem.heap.used", heap.getUsed());
    rpt.setLongMetric("mem.heap.init", heap.getInit());
    rpt.setLongMetric("mem.heap.max", heap.getMax());
    rpt.setLongMetric("mem.heap.committed", heap.getCommitted());

    MemoryUsage notheap = mem.getNonHeapMemoryUsage();
    rpt.setLongMetric("mem.other.used", notheap.getUsed());
    rpt.setLongMetric("mem.other.init", notheap.getInit());
    rpt.setLongMetric("mem.other.max", notheap.getMax());
    rpt.setLongMetric("mem.other.committed", notheap.getCommitted());

    // TODO (jon) process (not in java)
    // pid
    // user / group / effective group id.
    // ulimit / open handles (lsof)

    // runtime (subset)
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    rpt.setStringMetric("rt.vmname", rt.getVmName());
    rpt.setStringMetric("rt.vmvendor", rt.getVmVendor());
    rpt.setStringMetric("rt.vmversion", rt.getVmVersion());
    // TODO (jon) add date type
    rpt.setStringMetric("rt.starttime", new Date(rt.getStartTime()).toString());
    // current working dir.

    // TODO (jon) threads

    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

}
