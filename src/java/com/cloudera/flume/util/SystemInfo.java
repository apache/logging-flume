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
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;

import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.NetUtils;

/**
 * This is a quick and dirty first cut reporter that gets system resource info.
 * Not ideal but will get basic things quickly.
 */
public class SystemInfo implements Reportable {

  String name = "System";

  @Override
  public String getName() {
    return this.name;
  }

  public SystemInfo(String name) {
    this.name = name + "system-info";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());

    // os
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    rpt.setStringMetric("os.arch", os.getArch());
    rpt.setStringMetric("os.name", os.getName());
    rpt.setStringMetric("os.version", os.getVersion());
    rpt.setLongMetric("os.cpus", os.getAvailableProcessors());
    // load in last minute
    rpt.setDoubleMetric("os.load", os.getSystemLoadAverage());
    rpt.setStringMetric("hostname", NetUtils.localhost());

    // os (not in java)
    // iotop
    // swap mem
    // du / df
    // netsat
    // pid
    // uptime
    // network interface details
    // fs's
    // endian

    // windows an vmware stuff

    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }
}
