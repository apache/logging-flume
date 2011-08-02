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

package com.cloudera.flume.reporter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * This singleton manages an aggregate of reporters, and also sets up an http
 * server to display the contents of the reports (output as json, then use
 * javascript code to make pretty).
 * 
 * Open is only required to start the http server that displays information.
 * 
 * This is now thread safe.
 */
public class ReportManager implements Reportable {
  static Logger logger = Logger.getLogger(ReportManager.class.getName());
  final static ReportManager man = new ReportManager("root");

  Map<String, Reportable> reports = new HashMap<String, Reportable>();
  String name;

  private ReportManager(String name) {
    this.name = name;
  }

  public static ReportManager get() {
    return man;
  }

  /**
   * Registers a reportable object. If a Reportable with the same name is
   * already registered, it will be replaced.
   */
  public synchronized void add(Reportable r) {
    reports.put(r.getName(), r);
  }

  /**
   * Registers a set of reportable objects. If a Reportable with the same name
   * as one of those supplied is already registered, it will be replaced.
   */
  public synchronized void addAll(Collection<? extends Reportable> c) {
    for (Reportable r : c) {
      reports.put(r.getName(), r);
    }
  }

  /**
   * Returns the registered reportable with the given name, or null if none
   * exists.
   */
  public synchronized Reportable getReportable(String s) {
    return reports.get(s);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());
    for (Entry<String, Reportable> r : reports.entrySet()) {
      rpt.hierarchicalMerge(r.getKey(), r.getValue().getReport());
    }
    return rpt;
  }

  /**
   * Returns a map from reportable name to report for all reportables registered
   * with this ReportManager.
   */
  public synchronized Map<String, Reportable> getReportables() {
    return new HashMap<String, Reportable>(reports);
  }

  /**
   * Removes all registered reportables from the ReportManager.
   */
  public synchronized void clear() {
    reports.clear();
  }

  
  public synchronized void remove(Reportable r) {
    Preconditions.checkArgument(r != null, "Cannot remove null reportable");
    reports.remove(r.getName());
  }
}
