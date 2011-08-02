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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator returns a list of Search matches.
 */

class Searcher<T> implements Iterator<SearchResult<T>> {
  private SearchResult<T> currentResult;
  private AhoCorasick<T> tree;

  Searcher(AhoCorasick<T> tree, SearchResult<T> result) {
    this.tree = tree;
    this.currentResult = result;
  }

  public boolean hasNext() {
    return (this.currentResult != null);
  }

  public SearchResult<T> next() {
    if (!hasNext())
      throw new NoSuchElementException();
    SearchResult<T> result = currentResult;
    currentResult = tree.continueSearch(currentResult);
    return result;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

}
