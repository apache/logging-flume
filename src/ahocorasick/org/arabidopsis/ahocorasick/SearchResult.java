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

import java.util.Set;

/**
 * <p>
 * Holds the result of the search so far. Includes the outputs where the search
 * finished as well as the last index of the matching.
 * </p>
 * 
 * <p>
 * (Internally, it also holds enough state to continue a running search, though
 * this is not exposed for public use.)
 * </p>
 */
public class SearchResult<T> {
  State<T> lastMatchedState;
  byte[] bytes;
  int lastIndex;

  SearchResult(State<T> s, byte[] bs, int i) {
    this.lastMatchedState = s;
    this.bytes = bs;
    this.lastIndex = i;
  }

  /**
   * Returns a list of the outputs of this match.
   */
  public Set<T> getOutputs() {
    return lastMatchedState.getOutputs();
  }

  /**
   * Returns the index where the search terminates. Note that this is one byte
   * after the last matching character.
   */
  public int getLastIndex() {
    return lastIndex;
  }
}
