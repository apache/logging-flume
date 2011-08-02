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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.reporter.ReportEvent;
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
  final static Logger LOG =
      Logger.getLogger(BackOffFailOverSink.class.getName());

  final String A_PRIMARY = "sentPrimary";
  final String A_FAILS = "failsPrimary";
  final String A_BACKUPS = "sentBackups";

  EventSink primary;
  EventSink backup;
  AtomicLong primarySent = new AtomicLong();
  AtomicLong fails = new AtomicLong();
  AtomicLong backups = new AtomicLong();
  boolean primaryOk = false;
  boolean backupOpen = false;

  final BackoffPolicy backoffPolicy;

  public BackOffFailOverSink(EventSink primary, EventSink backup,
      BackoffPolicy backoff) {
    this.primary = primary;
    this.backup = backup;
    this.backoffPolicy = backoff;
  }

  public BackOffFailOverSink(EventSink primary, EventSink backup) {
    this(primary, backup, FlumeConfiguration.get().getFailoverInitialBackoff(),
        FlumeConfiguration.get().getFailoverMaxBackoff());
  }

  public BackOffFailOverSink(EventSink primary, EventSink backup,
      long initialBackoff, long maxBackoff) {
    this(primary, backup, new CappedExponentialBackoff(initialBackoff,
        maxBackoff));
  }

  @Override
  public void append(Event e) throws IOException {

    // we know primary has already failed, and it is time to retry it.
    if (!primaryOk) {

      if (backoffPolicy.isRetryOk()) {
        // attempt to recover primary.
        IOException ioe = tryOpenPrimary();
        if (ioe != null) {
          // reopen attempt failed, add to backoff, and fall back to backup. if
          // backup failes, give up
          backoffPolicy.backoff();
          try {
            // fall back onto secondary after primary retry failure.
            backup.append(e); // attempt on secondary.
            backups.incrementAndGet();
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
        return;
      }
    }

    // ideal case -- primary is open.
    try {
      primary.append(e);
      primarySent.incrementAndGet();
      primaryOk = true;
      backoffPolicy.reset(); // successfully sent with primary, so backoff
      // isreset
      return;
    } catch (IOException ioe3) {
      fails.incrementAndGet();
      primaryOk = false;
      backoffPolicy.backoff();
      backup.append(e);
      backups.incrementAndGet();
    }
  }

  @Override
  public void close() throws IOException {
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

    if (exs.size() != 0)
      throw MultipleIOException.createIOException(exs);

    return;
  }

  IOException tryOpenPrimary() {
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
  public void open() throws IOException {
    IOException priEx = tryOpenPrimary();

    try {
      // this could be opened lazily
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
  public String getName() {
    return "BackoffFailover";
  }

  @Override
  public ReportEvent getReport() {

    ReportEvent e = super.getReport();

    e.hierarchicalMerge(getName() + "[primary]", primary.getReport());
    e.hierarchicalMerge(getName() + "[backup]", backup.getReport());

    Attributes.setLong(e, A_FAILS, fails.get());
    Attributes.setLong(e, A_BACKUPS, backups.get());
    Attributes.setLong(e, A_PRIMARY, primarySent.get());

    return e;
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
