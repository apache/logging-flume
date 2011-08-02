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
package com.cloudera.flume.faults;

/**
 * This is a generic mechanism to eventually exhaust the program/os/machine's
 * resources. Subclasses implement the increment method which will incrementally
 * consume (and not release) more resources.
 */
public abstract class ResourceHog {
  long delay;
  boolean random;

  public ResourceHog(int delay, boolean random) {
    this.delay = delay;
    this.random = true;
  }

  abstract public void increment();

  // This will eventually blowup the process with some kind of resource
  // exhaustion
  public void exhaust() {
    try {
      while (true) {
        long wait = (long) (delay * (random ? Math.random() : 1.0));
        Thread.sleep(wait);
        increment();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void start() {
    new Thread(this.getClass().getName()) {
      public void run() {
        exhaust();
      }
    }.start();
  }

}
