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
package com.cloudera.flume.reporter.sampler;

import java.io.IOException;
import java.util.Random;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This sampler works by specifying the probability (double between 0.0 and 1.0)
 * that a message will be forwarded to the decorated sink. (e.g. .90 means 9 out
 * of 10 message will be forwarded)
 */
public class ProbabilitySampler<S extends EventSink> extends
    EventSinkDecorator<S> {

  double prob;
  Random rand;

  public ProbabilitySampler(S s, double prob, long seed) {
    super(s);
    this.prob = prob;
    this.rand = new Random(seed);
  }

  public ProbabilitySampler(S s, double prob) {
    this(s, prob, Clock.unixTime());
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (rand.nextDouble() < prob) {
      getSink().append(e);
    }
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 1 || argv.length == 2,
            "usage: probabilitySampler(prob[, seed])");

        double prob = Double.parseDouble(argv[0]);
        long seed = Clock.unixTime();
        if (argv.length == 2) {
          seed = Long.parseLong(argv[1]);
        }

        return new ProbabilitySampler<EventSink>(null, prob, seed);
      }

    };
  }
}
