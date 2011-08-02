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
 * Represents an EdgeList by using a single array. Very fast lookup (just an
 * array access), but expensive in terms of memory.
 */

class DenseEdgeList<T> implements EdgeList<T> {
  private State<T>[] array;

  @SuppressWarnings("unchecked")
  public DenseEdgeList() {
    this.array = new State[256];
    for (int i = 0; i < array.length; i++)
      this.array[i] = null;
  }

  /**
   * Helps in converting to dense representation.
   */
  public static <T> DenseEdgeList<T> fromSparse(SparseEdgeList<T> list) {
    byte[] keys = list.keys();
    DenseEdgeList<T> newInstance = new DenseEdgeList<T>();
    for (int i = 0; i < keys.length; i++) {
      newInstance.put(keys[i], list.get(keys[i]));
    }
    return newInstance;
  }

  public State<T> get(byte b) {
    return this.array[(int) b & 0xFF];
  }

  public void put(byte b, State<T> s) {
    this.array[(int) b & 0xFF] = s;
  }

  public byte[] keys() {
    int length = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != null)
        length++;
    }
    byte[] result = new byte[length];
    int j = 0;
    for (int i = 0; i < array.length; i++) {
      if (array[i] != null) {
        result[j] = (byte) i;
        j++;
      }
    }
    return result;
  }

}
