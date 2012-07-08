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
package com.cloudera.flume.agent;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.agent.MemoryMonitor.Listener;

/**
 * This tests two cases -- the first where memory is exhausted but in a GC'able
 * state. The trigger function gc's and then continues
 * 
 * The second is a case where gc will not free up enough memory. We mark failure
 * here (in the real use case, we would exit hard).
 * 
 */
public class TestMemoryMonitor {
  static double threshold = .50;

  volatile boolean failed = false;

  /**
   * This forces a GC when memory is near a threshold. If after the gc we are
   * still above the threshold mark with the failed condition.
   */
  @Test
  public void testNormalExhaust() {
    MemoryMonitor.setPercentageUsageThreshold(threshold);
    final MemoryMonitor mem = MemoryMonitor.getMemoryMonitor();
    failed = false;

    Listener l = new Listener() {
      @Override
      public void memoryUsageLow(long usedMemory, long maxMemory) {
        System.out.printf("pre  gc: memory usage %2.2f%%, %d used, %d max\n",
            ((double) usedMemory * 100) / (double) maxMemory,
            usedMemory / 1024 / 1024, maxMemory / 1024 / 1024);
        System.gc();

        long umem = mem.getMemUsage();
        long mmem = mem.getMemMax();

        double percent = (umem) / mmem;
        System.out.printf("post gc: memory usage %2.2f%%, %d used, %d max\n",
            percent * 100, umem / 1024 / 1024, mmem / 1024 / 1024);

        if (percent > threshold)
          failed = true; // this happens in some random thread.
      }

    };
    mem.addListener(l);

    // exhaust
    for (int i = 1; i < 1000; i++) {
      // allocated 10 megs at a time until it blows up.
      byte[] b = new byte[10 * 1024 * 1024];
      b[0] = 0;
      if (failed) {
        Assert.fail("Out of memory!");
      }
    }
    mem.removeListener(l);
  }

  @Test
  public void testExhaustRecover() {
    MemoryMonitor.setPercentageUsageThreshold(threshold);
    final MemoryMonitor mem = MemoryMonitor.getMemoryMonitor();
    failed = false;

    Listener l = new Listener() {
      @Override
      public void memoryUsageLow(long usedMemory, long maxMemory) {
        System.out.printf("pre  gc: memory usage %2.2f%%, %d used, %d max\n",
            ((double) usedMemory * 100) / (double) maxMemory,
            usedMemory / 1024 / 1024, maxMemory / 1024 / 1024);
        // System.gc();
        Runtime.getRuntime().gc();

        long umem = mem.getMemUsage();
        long mmem = mem.getMemMax();

        double percent = ((double) umem * 100) / (double) mmem;
        System.out.printf("post gc: memory usage %2.2f%%, %d used, %d max\n",
            percent, umem / 1024 / 1024, mmem / 1024 / 1024);

        if (percent > (threshold * 100))
          failed = true; // this happens in some random thread.
      }

    };
    mem.addListener(l);

    // exhaust
    try {
      ArrayList<byte[]> data = new ArrayList<byte[]>();
      for (int i = 1; i < 1000; i++) {
        // allocated 10 megs at a time until it blows up -- keeping in a data
        // structure so it blows up.
        byte[] b = new byte[10 * 1024 * 1024];
        b[0] = 0;
        data.add(b);
        System.out.printf("Megs allocated %d\n", i * 10);
        if (failed) {
          throw new RuntimeException("out of memory");
        }
      }
    } catch (RuntimeException e) {
      // this is the expected case
      return;
    } catch (OutOfMemoryError oome) {
      Assert.fail("expected this to get caught and dealt with");
    } finally {
      mem.removeListener(l);
    }
    Assert.fail("expected a failure");
  }
}
