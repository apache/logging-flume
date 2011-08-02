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

package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.bloom.BloomSet;
import com.google.common.base.Preconditions;

/**
 * BloomCheckDeco records checksums of the body of all the events received in a
 * set represented as a bloom filter. This is intended to be downstream from a
 * BloomGeneratorDeco. The generator may eventually send a specially tagged
 * event with a with bloom filter's backing data. The generator's bloom filter
 * is then compared against the checker's. Ideally the incoming set is equal to
 * or a subset of this checker's bloom set.
 * 
 * Some metrics are reported by the decorator's getReport method.
 * 
 * Initially the decorator keeps track of the bloom inclusion state. It starts
 * as UNKNOWN. If a generator bloom set is a subset, the decorator takes on the
 * SUCCESS state. If at any time a bloom set fails the containment check, the
 * decorator enters and gets stuck in FAIL mode until it is closed and opened.
 * The number of successful and failed bloom checks are also recorded.
 */
public class BloomCheckDecorator extends EventSinkDecorator<EventSink> {
  public static final Logger LOG = LoggerFactory
      .getLogger(BloomCheckDecorator.class);
  protected BloomSet bloom;
  final int size; // size of bloom bit array in bytes
  final int hashes; // number of hashes per insertion/membership test
  final EventSink reportSink;

  public final static String A_SUCCESS = "bloomCheckSuccesses";
  public final static String A_FAILS = "bloomCheckFails";
  public final static String A_STATE = "bloomCheckState";

  public static enum BloomCheckState {
    UNKNOWN, // unknown means no bloom check msg received,
    SUCCESS, // all received bloom checks are ok and at least one bloock check
    // msg received.
    FAIL, // at least one received bloom check failed.
  };

  // metrics
  BloomCheckState state = BloomCheckState.UNKNOWN;
  int successCount = 0;
  int failCount = 0;

  /**
   * This checker must have the same size and # hash as the upstream
   * BloomGeneratorDeco.
   */
  public BloomCheckDecorator(EventSink s, int size, int hashes, EventSink rpt) {
    super(s);
    this.size = size;
    this.hashes = hashes;
    this.reportSink = rpt;
  }

  public BloomCheckDecorator(EventSink s, int size, int hashes) {
    super(s);
    this.size = size;
    this.hashes = hashes;
    this.reportSink = new NullSink();
  }

  /**
   * The default sink is null and must be set to be used.
   * 
   * This checker must have the same size and # hash as the upstream
   * BloomGeneratorDeco.
   */
  public BloomCheckDecorator(int size, int hashes) {
    this(null, size, hashes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void open() throws IOException, InterruptedException {
    bloom = new BloomSet(size, hashes);
    state = BloomCheckState.UNKNOWN;
    successCount = 0;
    failCount = 0;
    super.open();
    reportSink.open();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void append(Event e) throws IOException, InterruptedException {
    byte[] data = e.get(BloomGeneratorDeco.A_BLOOMSETDATA);
    // if has BloomSet Data is present
    if (data != null) {
      // process and then drop the event.
      BloomSet subset = new BloomSet(data);
      boolean contained = bloom.contains(subset);

      if (LOG.isDebugEnabled()) {
        LOG.debug("received bloom set: " + Arrays.toString(subset.getBytes()));
        LOG.debug("local bloom set:    " + Arrays.toString(bloom.getBytes()));
      }

      synchronized (this) {
        switch (state) {
        case UNKNOWN:
        case SUCCESS:
          state = contained ? BloomCheckState.SUCCESS : BloomCheckState.FAIL;
          break;

        case FAIL:
        default:
          state = BloomCheckState.FAIL;
          LOG
              .info("received bloom set was not contained by local set! entering FAILed state");
        }

        if (contained) {
          successCount++;
        } else {
          failCount++;
        }
      }

      ReportEvent rpt = getReport();
      LOG.info(rpt.toText());
      reportSink.append(e);
      // record info but do not pass the message on.
      return;
    }

    // track the message.
    BloomGeneratorDeco.includeEvent(bloom, e);
    // and then just send the data downstream
    super.append(e);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public void close() throws IOException, InterruptedException {
    reportSink.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent evt = super.getReport();
    evt.set(A_STATE, state.toString().getBytes());
    Attributes.setInt(evt, A_SUCCESS, successCount);
    Attributes.setInt(evt, A_FAILS, failCount);
    return evt;
  }

  /**
   * Builds a BloomCheckDeco with optional specified number of bits and number
   * of hash functions.
   */
  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length <= 3,
            "usage: bloomGen[(sz[,hashes[,rptSink]])]");
        int sz = 100000000; // default: 100M bits => 12.5MB
        int hashes = 2; // default: # of hashes per insert/lookup
        if (argv.length >= 1) {
          sz = Integer.parseInt(argv[0]);
        }
        if (argv.length >= 2) {
          hashes = Integer.parseInt(argv[1]);
        }
        EventSink rptSink = new NullSink();
        if (argv.length >= 3) {
          String rptSpec = argv[2];
          try {
            rptSink = new CompositeSink(ctx, rptSpec);
          } catch (FlumeSpecException e) {
            LOG.debug("failed to parse rpt spec", e);
            throw new IllegalArgumentException(e.getMessage());
          }
        }

        return new BloomCheckDecorator(null, sz, hashes, rptSink);
      }
    };
  }
}
