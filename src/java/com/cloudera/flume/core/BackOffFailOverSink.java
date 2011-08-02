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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CappedExponentialBackoff;
import com.cloudera.util.MultipleIOException;
import com.google.common.base.Preconditions;

/**
 * This failover sink initially opens the primary and the backup attempts to to
 * append to the primary. If the primary fails, it falls back and appends to the
 * secondary. When the next message appears, it will continue to send to the
 * secondary until an initialBackoff time has elapsed. Once this has elapsed, an
 * attempt to reopen the primary and append to the primary. If the primary fails
 * again, backoff adjusted and we fall back to the secondary again.
 * 
 * If we reach the secodary and it fails, the append calls will throw an
 * exception.
 * 
 * These can be chained if multiple failovers are desired. (failover to another
 * failover). To save on open handles or connections, one can wrap the secondary
 * with a lazyOpen decorator
 */
public class BackOffFailOverSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(BackOffFailOverSink.class);

  public static final String A_PRIMARY = "sentPrimary";
  public static final String A_FAILS = "failsPrimary";
  public static final String A_BACKUPS = "sentBackups";

  final EventSink primary;
  final EventSink backup;
  AtomicLong primarySent = new AtomicLong();
  AtomicLong fails = new AtomicLong();
  AtomicLong backups = new AtomicLong();
  boolean primaryOk = false;
  boolean backupOpen = false;

  final BackoffPolicy backoffPolicy;

  public BackOffFailOverSink(EventSink primary, EventSink backup,
      BackoffPolicy backoff) {
    Preconditions.checkNotNull(primary,
        "BackOffFailOverSink called with null primary");
    Preconditions.checkNotNull(backup,
        "BackOffFailOverSink called with null backup");
    this.primary = primary;
    this.backup = backup;
    this.backoffPolicy = backoff;
  }

  public BackOffFailOverSink(EventSink primary, EventSink backup) {
    this(primary, backup, FlumeConfiguration.get().getFailoverInitialBackoff(),
        FlumeConfiguration.get().getFailoverMaxSingleBackoff());
  }

  public BackOffFailOverSink(EventSink primary, EventSink backup,
      long initialBackoff, long maxBackoff) {
    this(primary, backup, new CappedExponentialBackoff(initialBackoff,
        maxBackoff));
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {

    // we know primary has already failed, and it is time to retry it.
    if (!primaryOk) {

      if (backoffPolicy.isRetryOk()) {
        // attempt to recover primary.
        IOException ioe = tryOpenPrimary();
        if (ioe != null) {
          // reopen attempt failed, add to backoff, and fall back to backup. if
          // backup fails, give up
          backoffPolicy.backoff();
          try {
            // fall back onto secondary after primary retry failure.
            backup.append(e); // attempt on secondary.
            backups.incrementAndGet();
            super.append(e);
            return;
          } catch (IOException ioe2) {
            // complete failure case.
            List<IOException> exceptions = new ArrayList<IOException>(2);
            exceptions.add(ioe);
            exceptions.add(ioe2);
            IOException mio = MultipleIOException.createIOException(exceptions);
            throw mio;
          }
        }
      } else {
        // not ready to retry, fall back to secondary.
        backup.append(e);
        backups.incrementAndGet();
        super.append(e);
        return;
      }
    }

    // ideal case -- primary is open.
    try {
      primary.append(e);
      primarySent.incrementAndGet();
      super.append(e);
      primaryOk = true;
      backoffPolicy.reset(); // successfully sent with primary, so backoff
      // isreset
      return;
    } catch (IOException ioe3) {
      LOG.info(ioe3.getMessage());
      fails.incrementAndGet();
      primaryOk = false;
      backoffPolicy.backoff();
      backup.append(e);
      backups.incrementAndGet();      
      super.append(e);
    }
  }

  @Override
  public void close() throws IOException, InterruptedException {
    List<IOException> exs = new ArrayList<IOException>(2);
    try {
      if (primaryOk)
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

  IOException tryOpenPrimary() throws InterruptedException {
    IOException priEx = null;

    try {
      if (!primaryOk) {
        primary.close();
        primary.open();
        primaryOk = true;
      }
    } catch (IOException ex) {
      try {
        primary.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      primaryOk = false;
      priEx = ex;
    }
    return priEx;
  }

  @Override
  public void open() throws IOException, InterruptedException {
    IOException priEx = tryOpenPrimary();

    if (Thread.currentThread().isInterrupted()) {
      LOG.error("Backoff Failover sink exited because of interruption");
      throw new IOException("Was interrupted, bailing out");
    }

    try {
      // this could be opened lazily
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

  public EventSink getPrimary() {
	  return primary;
  }
  
  public EventSink getBackup() {
	  return backup;
  }
  
  @Override
  public String getName() {
    return "BackoffFailover";
  }

  @Deprecated
  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();

    rpt.setLongMetric(A_FAILS, fails.get());
    rpt.setLongMetric(A_BACKUPS, backups.get());
    rpt.setLongMetric(A_PRIMARY, primarySent.get());

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
  public ReportEvent getMetrics() {
    ReportEvent e = super.getMetrics();

    e.setLongMetric(A_FAILS, fails.get());
    e.setLongMetric(A_BACKUPS, backups.get());
    e.setLongMetric(A_PRIMARY, primarySent.get());

    return e;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put("primary." + primary.getName(), primary);
    map.put("backup." + backup.getName(), backup);
    return map;
  }

  public long getFails() {
    return fails.get();
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
          return new BackOffFailOverSink(pri, sec);
        } catch (FlumeSpecException e) {
          LOG.warn("Spec parsing problem", e);
          throw new IllegalArgumentException("Spec parsing problem", e);
        }
      }

    };

  }
}
