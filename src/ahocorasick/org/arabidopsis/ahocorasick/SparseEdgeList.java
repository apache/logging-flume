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

/**
 * Linked list implementation of the EdgeList should be less memory-intensive.
 * 
 * jon: tweaked to be type generic
 */

class SparseEdgeList<T> implements EdgeList<T> {
  private Cons<T> head;

  public SparseEdgeList() {
    head = null;
  }

  public State<T> get(byte b) {
    Cons<T> c = head;
    while (c != null) {
      if (c.b == b)
        return c.s;
      c = c.next;
    }
    return null;
  }

  public void put(byte b, State<T> s) {
    this.head = new Cons<T>(b, s, head);
  }

  public byte[] keys() {
    int length = 0;
    Cons<T> c = head;
    while (c != null) {
      length++;
      c = c.next;
    }
    byte[] result = new byte[length];
    c = head;
    int j = 0;
    while (c != null) {
      result[j] = c.b;
      j++;
      c = c.next;
    }
    return result;
  }

  static private class Cons<T> {
    byte b;
    State<T> s;
    Cons<T> next;

    public Cons(byte b, State<T> s, Cons<T> next) {
      this.b = b;
      this.s = s;
      this.next = next;
    }
  }

}
