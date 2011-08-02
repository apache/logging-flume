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

import java.util.List;

import junit.framework.TestCase;

/**
 * Reservoir sampling extracts a certain number of samples from a set with
 * uniform probability without knowing how many elements are in the total set.
 */
public class TestReservoirSampler extends TestCase {

  public void testReserviorSampler() {
    Histogram<Integer> h = new Histogram<Integer>();
    for (int i = 0; i < 10000; i++) {
      ReservoirSampler<Integer> rs = new ReservoirSampler<Integer>(10, i);

      for (int j = 0; j < 100; j++) {
        rs.onNext(j);
      }

      rs.onCompleted(true);
      List<Integer> l = rs.sample();

      for (Integer k : l) {
        h.increment(k);
      }

    }

    System.out.println(h);
    for (int i = 0; i < 100; i++) {
      int j = h.get(i);
      // TODO (jon) it should be somewhere around 1000 hits per slot, but I'll
      // approximate with these values for now. There is a formula somewhere for
      // a more reasonable statistical bounds.
      assertTrue(j < 1150 && j > 850);
    }

  }

}
