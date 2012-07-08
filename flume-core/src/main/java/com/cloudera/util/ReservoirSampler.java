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
package com.cloudera.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Reservoir sampling is a method for getting a uniform random sampling of n
 * data elements from a stream of N data elements without a priori knowing the
 * total number of elements N.
 * 
 * This is based off of Algorithm R from this paper:
 * http://portal.acm.org/citation.cfm?id=3165
 * 
 * This implementation is uses a push-based observer instead of a pull based
 * iterator found in the paper. The "fast forwarding" methods in the paper are
 * not relevant to our application..
 */
public class ReservoirSampler<T> {

  int samples; // number of elements to sample.
  List<T> candidates; // the reservoir.
  int count = 0;
  boolean done = false;

  Random rand;

  public ReservoirSampler(int samples, long seed) {
    this.samples = samples;
    this.candidates = new ArrayList<T>(samples);
    this.rand = new Random(seed);
  }

  public ReservoirSampler(int samples) {
    this(samples, Clock.unixTime());
  }

  public void onCompleted(boolean b) {
    done = b;
  }

  public void onError(Exception e) {
    done = true;
  }

  public void onNext(T v) {
    if (done)
      return;

    if (candidates.size() < samples) {
      // for the first n elements.
      candidates.add(v);
      count++;
      return;
    }

    // do reservoir sampling.
    count++;
    // rand.nextDouble gets a pseudo random value between 0.0 and 1.0
    int replace = (int) Math.floor((double) count * rand.nextDouble());
    if (replace < samples) {
      // probability says replace.
      candidates.set(replace, v);
    }
    // else keep the current sample reservoir
  }

  /**
   * Returns an unmodifiable reference to the sample list.
   */
  public List<T> sample() {
    return Collections.unmodifiableList(candidates);
  }
  
  public void clear() {
    candidates.clear();
  }
}
