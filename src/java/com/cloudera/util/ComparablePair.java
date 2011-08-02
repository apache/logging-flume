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

/**
 * Augments Pair with the Comparable interface so that we may create sorted sets
 * of pairs. L and R types must implement Comparable.
 */
public class ComparablePair<L extends Comparable<L>, R extends Comparable<R>>
    extends Pair<L, R> implements Comparable<ComparablePair<L, R>> {

  /**
   * Default constructor - left and right may be null.
   */
  public ComparablePair(L left, R right) {
    super(left, right);
  }

  @Override
  /**
   * Compares this pair to another. (x,y) < (a,b) iff x < a || x = a && y < b
   * If x = null or a = null or x = a && (y == null || b == null) then this method
   * raises a NullPointerException
   */
  public int compareTo(ComparablePair<L, R> o) {
    if (o == null || getLeft() == null) {
      // As per
      // http://java.sun.com/j2se/1.5.0/docs/api/java/lang/Comparable.html
      throw new NullPointerException();
    }
    int ret = getLeft().compareTo(o.getLeft());
    if (ret == 0) {
      if (getRight() == null) {
        throw new NullPointerException();
      }
      ret = getRight().compareTo(o.getRight());
    }
    return ret;
  }
}
