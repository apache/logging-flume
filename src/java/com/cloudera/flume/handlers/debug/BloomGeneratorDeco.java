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

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.bloom.BloomSet;
import com.google.common.base.Preconditions;

/**
 * This decorator takes hashes of messages and then inserts them into a bloom
 * filter. On deco close, the bit map representation of the bloom filter is
 * transmittted into the stream.
 * 
 * A corresponding BloomChecker can track received message and can approximately
 * verify that all messages injected were included. If the generator sends
 * messages that aren't received by the checker, probabilistically this should
 * detect the omission.
 * 
 * The probability of a dropped message not being detected is equivalent to the
 * probability of that dropped message being a false positive if queried. (ie,
 * the addition of the message to the set added no new information). Wikipedia
 * says to use at a minimum of 9 bits per inserted item, and that extra hashes
 * roughly decrease the probability by an order of magnitude.
 * 
 */
public class BloomGeneratorDeco extends EventSinkDecorator<EventSink> {
  public static Logger LOG = Logger.getLogger(BloomGeneratorDeco.class);
  protected BloomSet bloom;
  final int size; // size of bloom bit array in bits
  final int hashes; // number of hashes per insertion/membership test

  public final static String A_BLOOMSETDATA = "bloomSetData";

  /**
   * This generator must have the same size and # hash as the downstream
   * BloomCheckDeco.
   */
  public BloomGeneratorDeco(EventSink s, int size, int hashes) {
    super(s);
    this.size = size;
    this.hashes = hashes;
  }

  /**
   * The default sink here is null and must be set by setSink before usage.
   * 
   * This generator must have the same size and # hash as the downstream
   * BloomCheckDeco.
   */

  public BloomGeneratorDeco(int size, int hashes) {
    this(null, size, hashes);
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException {
    bloom = new BloomSet(size, hashes);
    super.open();
  }

  /** {@inheritDoc} */
  @Override
  public void append(Event e) throws IOException {
    // take a hash of the bytes contents and add them to bloom filter.
    includeEvent(bloom, e);

    // then just send the data
    super.append(e);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    EventImpl e = new EventImpl(new byte[0]);
    addBloom(bloom, e);
    super.append(e);

    // then close
    super.close();
  }

  /**
   * Adds the hash of the event body into the specified bloom filter set.
   */
  static void includeEvent(BloomSet bloom, Event e) {
    // Arrays.hashCode results in the same value across all jdks/machines and
    // jvm instances. (e.getBody().hashcode() is not guaranteed to do this)
    int hash = Arrays.hashCode(e.getBody());
    bloom.addInt(hash);
  }

  /**
   * Adds the serialized bloom set as an attribute to the specified event.
   */
  static void addBloom(BloomSet bloom, Event e) throws IOException {
    // ship the serialize bloom filter
    e.set(A_BLOOMSETDATA, bloom.getBytes());
  }

  /**
   * Builds a BloomCheckDeco with optional specified number of bits and number
   * of hash functions.
   */
  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length <= 2,
            "usage: bloomCheck[(sz[,hashes])]");
        int sz = 100000000; // default: 100M bits.
        int hashes = 2; // default: # of hashes per insert/lookup
        if (argv.length >= 1) {
          sz = Integer.parseInt(argv[0]);
        }
        if (argv.length >= 2) {
          hashes = Integer.parseInt(argv[1]);
        }

        return new BloomGeneratorDeco(sz, hashes);
      }
    };
  }
}
