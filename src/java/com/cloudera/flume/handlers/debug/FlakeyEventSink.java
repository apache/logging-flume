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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This sink essentially acts as a pass through decorator, but will throw an
 * IOException with some probability. This is useful for testing a FailOverSink,
 * or for fault tolerance testing.
 */
public class FlakeyEventSink<S extends EventSink> extends EventSinkDecorator<S> {
  double prob; // probability of throwing an exception
  Random rand;

  public FlakeyEventSink(S s, double prob, long seed) {
    super(s);
    Preconditions.checkArgument(prob <= 1.0);
    Preconditions.checkArgument(prob >= 0.0);
    this.prob = prob;
    this.rand = new Random(seed);
  }

  @Override
  public void append(Event e) throws IOException , InterruptedException {
    if (rand.nextDouble() < prob) {
      throw new IOException("flakiness struck and caused a failure");
    }
    super.append(e);
  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        if (argv.length == 0) {
          throw new IllegalArgumentException(
              "usage: flakey(failprobability,[seed])");
        }
        double d = Double.parseDouble(argv[0]);
        long seed = 0;
        if (argv.length > 1) {
          seed = Integer.parseInt(argv[1]);
        } else {
          seed = System.currentTimeMillis();
        }
        return new FlakeyEventSink<EventSink>(null, d, seed);
      }

    };
  }

}
