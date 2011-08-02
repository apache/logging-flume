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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

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
public class FailOverSink implements EventSink, Reportable {
  final static Logger LOG = Logger.getLogger(FailOverSink.class.getName());

  // makes sure this doesn't cause conflict when constructor called
  // concurrently.
  static AtomicInteger uniq = new AtomicInteger();

  int suffix;
  EventSink primary;
  EventSink backup;
  long fails;
  long backups;
  boolean primaryOpen = false;
  boolean backupOpen = false;

  final String name;
  final String R_FAILS;
  final String R_BACKUPS;

  public FailOverSink(EventSink primary, EventSink backup) {
    this.primary = primary;
    this.backup = backup;
    suffix = uniq.getAndIncrement();
    this.name = "failover_" + suffix;
    this.R_FAILS = "rpt." + name + ".fails";
    this.R_BACKUPS = "rpt." + name + ".backups";
  }

  @Override
  public void append(Event e) throws IOException {
    if (primaryOpen) {
      try {
        primary.append(e);
        return;
      } catch (IOException ex) {
        fails++;
      }
    }
    backup.append(e);
    backups++;
  }

  @Override
  public void close() throws IOException {
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

    if (exs.size() != 0)
      throw MultipleIOException.createIOException(exs);

    return;
  }

  @Override
  public void open() throws IOException {
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
        IOException mioe =
            MultipleIOException.createIOException(Arrays.asList(priEx, ex));
        throw mioe;
      }
      // if the primary was ok, just continue. (if primary fails, will attempt
      // to open backup again before falling back on it)
    }

  }

  @Override
  public ReportEvent getReport() {
    ReportEvent e = new ReportEvent(name);
    Attributes.setLong(e, R_FAILS, fails);
    Attributes.setLong(e, R_BACKUPS, backups);
    return e;
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
