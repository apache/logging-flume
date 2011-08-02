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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;

/**
 * A synthetic source that just creates random events of random size and returns
 * them.
 * 
 * 'open' resets the source's seed and will generate essentially the same stream
 * of events.
 */
public class SynthSourceRndSize extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(SynthSourceRndSize.class);

  final long total;
  final long seed;
  final Random rand;
  long count = 0;
  final int minBodySize;
  final int maxBodySize;

  public SynthSourceRndSize(long total, int lowLimit, int upLimit) {
    this(total, lowLimit, upLimit, Clock.unixTime());

  }

  public SynthSourceRndSize(long total, int minsize, int maxsize, long seed) {
    this.rand = new Random(seed);
    this.seed = seed;
    this.minBodySize = minsize;
    this.maxBodySize = maxsize;
    this.total = count;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing SynthSourceRandSize");
  }

  @Override
  public Event next() throws IOException {
    if (count >= total && total != 0)
      return null;// end marker if gotten to count

    int range = this.maxBodySize - this.minBodySize;
    int size = this.minBodySize + (range > 0 ? this.rand.nextInt(range) : 0);
    count++;
    byte[] data = new byte[size];
    rand.nextBytes(data);
    Event e = new EventImpl(data);
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    LOG.info("Resetting count and seed; openingSynthSourceRandSize");
    // resetting the seed
    count = 0;
    rand.setSeed(seed);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        int minSize = 0;
        int maxSize = 10;
        long total = 0;

        if (argv.length != 3) {
          throw new IllegalArgumentException(
              "usage: synthrndsize(count=0, minsize=0, maxsize=10) // count=0 infinite");
        }
        total = Long.parseLong(argv[0]);
        minSize = Integer.parseInt(argv[1]);
        maxSize = Integer.parseInt(argv[2]);
        return new SynthSourceRndSize(total, minSize, maxSize);
      }
    };
  }
}
