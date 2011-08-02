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
package com.cloudera.util.consistenthash;

import java.util.List;

/**
 * Interface for callbacks that note when keys have been moved to different
 * nodes in the consistent hash. This allows for incremental changes (not
 * necessarily just for consistent hash bin cardinality changes)
 */
public interface MoveHandler<K, V> {
  public void moved(K from, K to, List<V> values);

  public void rebuild(K key, List<V> allVals);
}
