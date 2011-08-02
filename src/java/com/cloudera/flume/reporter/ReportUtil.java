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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These are utility functions for dealing with ReportsEvents.
 */
public class ReportUtil {
  final public static Logger LOG = LoggerFactory.getLogger(ReportUtil.class);

  /**
   * Convert a flume report in to a jettison JSONObject.
   **/
  public static JSONObject toJSONObject(ReportEvent rpt) throws JSONException {
    JSONObject ret = new JSONObject();
    if (rpt == null) {
      LOG.warn("Attempted to create json object from null event");
      return ret;
    }

    for (Entry<String, Long> e : rpt.getAllLongMetrics().entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }

    for (Entry<String, Double> e : rpt.getAllDoubleMetrics().entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }

    for (Entry<String, String> e : rpt.getAllStringMetrics().entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }
    return ret;
  }

  /**
   * Returns an empty immutable map of children reports.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Reportable> noChildren() {
    return (Map<String, Reportable>) Collections.EMPTY_MAP;
  }

  /**
   * Gets a map from name to instance of all the listed Reportable's children.
   */
  public static Map<String, Reportable> subReports(Reportable... rtbls) {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    for (Reportable r : rtbls) {
      if (r == null) {
        LOG.warn("Had a null reportable when generating a subreport");
        continue;
      }
      map.put(r.getName(), r);
    }
    return map;
  }

  /**
   * Get a recursively flattened metrics report. If the reportable is null
   * this returns null.
   */
  public static ReportEvent getFlattenedReport(Reportable r) {
    if (r == null) {
      return null;
    }

    ReportEvent rpt = r.getMetrics();
    Map<String, Reportable> subs = r.getSubMetrics();
    for (Entry<String, Reportable> e : subs.entrySet()) {
      String name = e.getKey();
      ReportEvent subRpt = getFlattenedReport(e.getValue());
      rpt.mergeWithPrefix(name + ".", subRpt);
    }
    return rpt;
  }

}
