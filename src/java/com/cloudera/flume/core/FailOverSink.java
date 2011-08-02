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
package com.cloudera.flume.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.MultipleIOException;
import com.google.common.base.Preconditions;

/**
 * This sink always attempts to append to the primary. If the primary returns a
 * exception, it increments a failure counter and then appends to the backup.
 * These can be chained if multiple failovers are desired. (failover to another
 * failover)
 * 
 * This sink is very simple -- it will reattempt to open after every failure.
 * Most folks will want to use BackOffFailOver. This remains because it is so
 * much easier for testing.
 */
public class FailOverSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(FailOverSink.class);

  // makes sure this doesn't cause conflict when constructor called
  // concurrently.
  static AtomicInteger uniq = new AtomicInteger();

  int suffix;
  final EventSink primary;
  final EventSink backup;
  long fails;
  long backups;
  boolean primaryOpen = false;
  boolean backupOpen = false;

  final String name;
  public static final String R_FAILS = "fails";
  public static final String R_BACKUPS = "backups";

  public FailOverSink(EventSink primary, EventSink backup) {
    Preconditions.checkNotNull(primary);
    Preconditions.checkNotNull(backup);
    this.primary = primary;
    this.backup = backup;
    suffix = uniq.getAndIncrement();
    this.name = "failover_" + suffix;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (primaryOpen) {
      try {
        primary.append(e);
        super.append(e);
        return;
      } catch (IOException ex) {
        LOG.info("attempt to use primary failed: " + ex.getMessage());
        fails++;
      }
    }
    backup.append(e);
    backups++;
    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    List<IOException> exs = new ArrayList<IOException>(2);
    try {
      if (primaryOpen)
        primary.close();
    } catch (IOException ex) {
      exs.add(ex);
    }

    try {
      if (backupOpen)
        backup.close();
    } catch (IOException ex) {
      exs.add(ex);
    }

    if (exs.size() != 0) {
      throw MultipleIOException.createIOException(exs);
    }
  }

  @Override
  public void open() throws IOException, InterruptedException {
    IOException priEx = null;
    try {
      primary.open();
      primaryOpen = true;
    } catch (IOException ex) {
      primaryOpen = false;
      priEx = ex;
    }

    try {
      backup.open();
      backupOpen = true;
    } catch (IOException ex) {
      backupOpen = false;
      if (priEx != null) {
        // both failed, throw multi exception
        IOException mioe = MultipleIOException.createIOException(Arrays.asList(
            priEx, ex));
        throw mioe;
      }
      // if the primary was ok, just continue. (if primary fails, will attempt
      // to open backup again before falling back on it)
    }

  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = super.getMetrics();
    rpt.setLongMetric(R_FAILS, fails);
    rpt.setLongMetric(R_BACKUPS, backups);
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put("primary." + primary.getName(), primary);
    map.put("backup." + backup.getName(), backup);
    return map;
  }

  @Deprecated
  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(R_FAILS, fails);
    rpt.setLongMetric(R_BACKUPS, backups);
    return rpt;
  }

  @Deprecated
  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    primary.getReports(namePrefix + getName() + ".primary.", reports);
    backup.getReports(namePrefix + getName() + ".backup.", reports);
  }

  @Override
  public String getName() {
    return name;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 2);
        String primary = argv[0];
        String secondary = argv[1];
        try {
          EventSink pri = new CompositeSink(context, primary);
          EventSink sec = new CompositeSink(context, secondary);
          return new FailOverSink(pri, sec);
        } catch (FlumeSpecException e) {
          LOG.warn("Spec parsing problem", e);
          throw new IllegalArgumentException("Spec parsing problem", e);
        }
      }

    };
  }
}
