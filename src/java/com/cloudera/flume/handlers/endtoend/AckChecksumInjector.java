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
import java.util.zip.CRC32;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;
import com.google.common.base.Preconditions;

/**
 * This first sends a tag/checksum start message with the initial time stamp as
 * the checksum. Then it tags each message that passes through with a hash of
 * the message body. When closed, it sends a end message that has a summarized
 * hash. (XOR of checksums of the messages).
 * 
 * Rationale: XORing checksums is commutative and thus tolerant of messages that
 * come in the wrong order. This is assumed to be at a source and that all
 * messages pass through this.
 */
public class AckChecksumInjector<S extends EventSink> extends
    EventSinkDecorator<S> {

  public final static String ATTR_ACK_HASH = "AckChecksum";
  public final static String ATTR_ACK_TYPE = "AckType";
  public final static String ATTR_ACK_TAG = "AckTag";
  public final static byte[] CHECKSUM_MSG = "msg".getBytes(CharEncUtils.RAW);
  public final static byte[] CHECKSUM_START = "beg".getBytes(CharEncUtils.RAW);
  public final static byte[] CHECKSUM_STOP = "end".getBytes(CharEncUtils.RAW);

  // TODO (jon) consult with someone to make sure this is reasonable.
  // Another idea is to use a bloom filter and use its bitmap as a signature.
  CRC32 chk = new CRC32();
  // TODO (jon) switch to a different hash, like Paul Hsieh's SuperFastHash.
  // (no relation). This crc32 checksum actually only is 32 bits, so I'm
  // wasting space with 64 bits.

  long checksum;
  final byte[] tag;
  AckListener listener; // send notification to external objects

  public AckChecksumInjector(S s, byte[] tag, AckListener an) {
    super(s);
    // Although always currently called with tag == someString.getBytes(),
    // cloning is better practice.
    this.tag = tag.clone();
    this.listener = an;
    checksum = 0;
  }

  /**
   * This is only for testing.
   */
  public AckChecksumInjector(S s) {
    this(s, (NetUtils.localhost() + Clock.nanos()).getBytes(),
        new AckListener.Empty());
  }

  /**
   * Open event starts with a random value that the checksum will be based off
   * of.
   * 
   * Use the host and the nanos as a tag at the collector side.
   */
  private Event openEvent() {
    Event e = new EventImpl(new byte[0]);
    e.set(ATTR_ACK_TYPE, CHECKSUM_START);
    checksum = e.getTimestamp();
    e.set(ATTR_ACK_HASH, ByteBuffer.allocate(8).putLong(checksum).array());
    e.set(ATTR_ACK_TAG, tag);

    return e;
  }

  /**
   * Close events has the cumulative checksum value
   */
  private Event closeEvent() {
    Event e = new EventImpl(new byte[0]);
    e.set(ATTR_ACK_TYPE, CHECKSUM_STOP);
    e.set(ATTR_ACK_HASH, ByteBuffer.allocate(8).putLong(checksum).array());
    e.set(ATTR_ACK_TAG, tag);
    return e;
  }

  /**
   * Send open event after open
   */
  public void open() throws IOException {
    super.open();
    super.append(openEvent()); // purposely using old append
    listener.start(new String(tag));
  }

  /**
   * Send close event before close
   */
  public void close() throws IOException {
    super.append(closeEvent()); // purposely using old append
    super.close();
    listener.end(new String(tag));
  }

  /**
   * Calculate the crc based on the body of the message and xor it into the
   * checksum.
   */
  public void append(Event e) throws IOException {
    chk.reset();
    chk.update(e.getBody());
    long curchk = chk.getValue();
    checksum ^= curchk; // update but do not send.

    e.set(ATTR_ACK_TYPE, CHECKSUM_MSG);
    e.set(ATTR_ACK_TAG, tag);
    e.set(ATTR_ACK_HASH, ByteBuffer.allocate(8).putLong(curchk).array());
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: ackInjector");
        AckListener queuer = FlumeNode.getInstance().getAckChecker()
            .getAgentAckQueuer();
        return new AckChecksumInjector<EventSink>(null,
            (NetUtils.localhost() + Clock.nanos()).getBytes(), queuer);
      }

    };
  }
}
