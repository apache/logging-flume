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
package org.arabidopsis.ahocorasick;

import java.util.ArrayList;

/**
 * Quick-and-dirty queue class. Essentially uses two lists to represent a queue.
 */
class Queue<T> {
  ArrayList<State<T>> l1;
  ArrayList<State<T>> l2;

  public Queue() {
    l1 = new ArrayList<State<T>>();
    l2 = new ArrayList<State<T>>();
  }

  public void add(State<T> s) {
    l2.add(s);
  }

  public boolean isEmpty() {
    return l1.isEmpty() && l2.isEmpty();
  }

  public State<T> pop() {
    if (isEmpty())
      throw new IllegalStateException();
    if (l1.isEmpty()) {
      for (int i = l2.size() - 1; i >= 0; i--)
        l1.add(l2.remove(i));
      assert l2.isEmpty();
      assert !l1.isEmpty();
    }
    return l1.remove(l1.size() - 1);
  }
}
