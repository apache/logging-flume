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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;

/**
 * A synthetic source that just creates random events of specified size and
 * returns them.
 * 
 * 'open' resets the source's seed and will generate essentially the same stream
 * of events.
 */
public class SynthSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(SynthSource.class);

  final int size;
  final long total;
  final long seed;
  final Random rand;
  long count = 0;

  public SynthSource(long count, int size) {
    this(count, size, Clock.unixTime());
  }

  public SynthSource(long count, int size, long seed) {
    this.rand = new Random(seed);
    this.seed = seed;
    this.size = size;
    this.total = count;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing SynthSource(" + total + ", " + size + " )");
  }

  @Override
  public Event next() throws IOException {
    if (count >= total && total != 0)
      return null;// end marker if gotten to count
    count++;
    byte[] data = new byte[size];
    rand.nextBytes(data);
    Event e = new EventImpl(data);
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    LOG.info("Resetting count and seed; openingSynthSource(" + total + ", "
        + size + " )");
    // resetting the seed
    count = 0;
    rand.setSeed(seed);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        int size = 150;
        long count = 0;
        if (argv.length > 2) {
          throw new IllegalArgumentException(
              "usage: synth([count=0 [,randsize=150]]) // count=0 infinite");
        }
        if (argv.length >= 1) {
          count = Long.parseLong(argv[0]);
        }
        if (argv.length >= 2)
          size = Integer.parseInt(argv[1]);
        return new SynthSource(count, size);
      }

    };
  }

}
