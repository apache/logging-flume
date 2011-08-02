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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This tracks ack on batches of events.
 * 
 * This looks for and tracks begin and end message for ack tagged values.
 * 
 * Will this be a reasonable size? 8 bytes for the checksum per machine coming
 * in. With high estimates of roughly 1000 machines, each with 5 connections,
 * with lets say 20 outstanding ack groups (64MB or roughly 10-20 seconds of
 * buffering) means 800 KB. No big deal.
 * 
 * Let say 1MB group, 50 MB, This is still manageable.
 */
public class AckChecksumChecker<S extends EventSink> extends
    EventSinkDecorator<S> {
  static final Logger LOG = LoggerFactory.getLogger(AckChecksumChecker.class);

  final static public String A_ACK_STARTS = "ackStarts";
  final static public String A_ACK_ENDS = "ackEnds";
  final static public String A_ACK_FAILS = "ackFails";
  final static public String A_ACK_SUCCESS = "ackSuccesses";
  final static public String A_ACK_UNEXPECTED = "ackUnexpected";

  AtomicLong ackStarts = new AtomicLong();
  AtomicLong ackEnds = new AtomicLong();
  AtomicLong ackFails = new AtomicLong();
  AtomicLong ackSuccesses = new AtomicLong();

  // TODO (jon) this is very inefficient right now.
  Map<String, Long> partial = new HashMap<String, Long>();

  final AckListener listener;
  long unstarted = 0; // number of events that didn't have a start event.

  public AckChecksumChecker(S s, AckListener l) {
    super(s);
    Preconditions.checkNotNull(l);
    this.listener = l;
  }

  public AckChecksumChecker(S s) {
    super(s);
    // do nothing listener
    this.listener = new AckListener() {
      @Override
      public void end(String group) {
        LOG.info("ended " + group);
      }

      @Override
      public void err(String group) {
        LOG.info("erred " + group);

      }

      @Override
      public void start(String group) {
        LOG.info("start " + group);

      }

      @Override
      public void expired(String group) throws IOException {
        LOG.info("expired " + group);
      }
    };
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    byte[] btyp = e.get(AckChecksumInjector.ATTR_ACK_TYPE);

    if (btyp == null) {
      // pass through if has no checksumming tags
      super.append(e);
      return;
    }

    byte[] btag = e.get(AckChecksumInjector.ATTR_ACK_TAG);
    byte[] bchk = e.get(AckChecksumInjector.ATTR_ACK_HASH);
    String k = new String(btag);

    if (Arrays.equals(btyp, AckChecksumInjector.CHECKSUM_START)) {
      LOG.info("Starting checksum group called " + k);
      // Checksum Start marker: create new partial
      long newchk = ByteBuffer.wrap(bchk).getLong();
      LOG.info("initial checksum is " + Long.toHexString(newchk));
      partial.put(k, newchk);
      ackStarts.incrementAndGet();
      listener.start(k);

      return;
    } else if (Arrays.equals(btyp, AckChecksumInjector.CHECKSUM_STOP)) {
      LOG.info("Finishing checksum group called '" + k + "'");
      ackEnds.incrementAndGet();
      // Checksum stop marker: move from partial to done
      Long chksum = partial.get(k);
      if (chksum == null) {
        LOG.error("checksum failed");
        listener.err(k);
        ackFails.incrementAndGet();
        return;
      }
      long endchk = ByteBuffer.wrap(bchk).getLong();
      LOG.debug("final checksum is " + Long.toHexString(endchk)
          + " stop checksum is " + Long.toHexString(chksum));
      if ((chksum ^ endchk) != 0) {
        // There was a problem.

        LOG.warn("[ Thread " + Thread.currentThread().getId()
            + " ] Some component of msg group was lost or duped " + k);
        listener.err(k);
        ackFails.incrementAndGet();
        return;
      }

      LOG.info("Checksum succeeded " + Long.toHexString(chksum));
      listener.end(k);
      ackSuccesses.incrementAndGet();
      partial.remove(k);
      LOG.info("moved from partial to complete " + k);
      return;
    }

    // normal case, just an update the checksum.
    CRC32 chk = new CRC32();
    chk.reset();
    chk.update(e.getBody());
    long chkVal = chk.getValue();

    if (chkVal != ByteBuffer.wrap(bchk).getLong()) {
      LOG.warn("check sum does not match!");
    }
    super.append(e);

    // only do this after we have successfully sent the event.
    synchronized (partial) {
      Long chks = partial.get(k);
      if (chks == null) {
        // throw new IOException("Ack tag '" + k + "' was not started: ");
        unstarted++;
        return;
      }

      // update checksum.
      long checksum = partial.get(k);
      checksum ^= chkVal;
      partial.put(k, checksum);
    }
  }

  @Override
  public void close() throws IOException, InterruptedException {
    super.close();
    if (partial.size() != 0) {
      LOG.warn("partial acks abandoned: " + partial);
    }
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    rpt.setLongMetric(A_ACK_FAILS, ackFails.get());
    rpt.setLongMetric(A_ACK_SUCCESS, ackSuccesses.get());
    rpt.setLongMetric(A_ACK_STARTS, ackStarts.get());
    rpt.setLongMetric(A_ACK_ENDS, ackEnds.get());
    rpt.setLongMetric(A_ACK_UNEXPECTED, unstarted);
    return rpt;
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: ackChecker");
        return new AckChecksumChecker<EventSink>(null, FlumeNode.getInstance()
            .getCollectorAckListener());
      }

    };
  }

}
