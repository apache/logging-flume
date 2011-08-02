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
import java.util.Random;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.CharEncUtils;
import com.cloudera.util.Clock;

/**
 * A synthetic source that just creates a specified number of random events with
 * a specified number of attributes with specified attribute size, and specified
 * value size.
 * 
 * 'open' resets the source's seed and will generate essentially the same stream
 * of events.
 */
public class AttrSynthSource extends EventSource.Base {
  final static Logger LOG = Logger.getLogger(AttrSynthSource.class.getName());

  final long msgs;
  final int attrs;
  final int valSz;
  final int attrSz;
  final String[] attrNames;
  final Random rand;
  final long seed;
  long count = 0;

  public AttrSynthSource(long msgs, int attrs, int attrsz, int valsz, long seed) {
    this.msgs = msgs;
    this.seed = seed;
    this.rand = new Random(seed);
    this.attrs = attrs;
    this.attrSz = attrsz;
    this.valSz = valsz;

    // generate random but consistent for every event attributes.
    this.attrNames = new String[attrs];
    for (int i = 0; i < attrs; i++) {
      attrNames[i] = getRandASCIIString(attrSz);
    }

  }

  /**
   * This is not meant to be efficient.
   */
  String getRandASCIIString(int len) {
    byte[] bytes = new byte[len];
    rand.nextBytes(bytes);
    String s = new String(bytes, CharEncUtils.RAW);
    return s.substring(0, len);

  }

  @Override
  public void close() throws IOException {
    LOG.info("closing AttrSynthSource(" + msgs + ", " + attrs + ", " + attrSz
        + ", " + valSz + " )");
  }

  @Override
  public Event next() throws IOException {
    if (count >= msgs)
      return null;// end marker if gotten to count
    count++;

    Event e = new EventImpl(new byte[0]);
    for (String a : attrNames) {
      byte[] val = new byte[valSz];
      rand.nextBytes(val);
      e.set(a, val);

    }

    return e;
  }

  @Override
  public void open() throws IOException {
    LOG.info("opening AttrSynthSource(" + msgs + ", " + attrs + ", " + attrSz
        + ", " + valSz + " )");
    // reset random num gen and count.
    count = 0;
    rand.setSeed(seed);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        if (argv.length < 4 || argv.length > 5) {
          throw new IllegalArgumentException(
              "usage: attrsynth(count, attrs, attrSz, valSz[,rseed=curtime]) // count=0 infinite");
        }

        long count = Integer.parseInt(argv[0]);
        int attrs = Integer.parseInt(argv[1]);
        int attrSz = Integer.parseInt(argv[2]);
        int valSz = Integer.parseInt(argv[3]);

        long seed = Clock.unixTime();
        if (argv.length >= 4) {
          seed = Integer.parseInt(argv[4]);
        }
        return new AttrSynthSource(count, attrs, attrSz, valSz, seed);
      }
    };
  }
}
