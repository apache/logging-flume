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

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * This is a simple LRU cache implementation. It uses SoftReferences that allows
 * the GC can collect memory from here if it needs memory.
 * 
 * This assumes writes to this are going to happen more often than reads: insert
 * are O(1), but lookups are O(n).
 * 
 * (A different cache implementation could make it both insert and lookup O(log
 * n).)
 */
public class Cache<K, V> {

  final int maxsize;
  LinkedList<K> fifo; // TODO (jon) consider treemap
  Map<K, SoftReference<V>> map; // TODO (jon) consider WeakHashMap

  public Cache(int maxsize) {
    this.maxsize = maxsize;
    this.fifo = new LinkedList<K>();
    this.map = new HashMap<K, SoftReference<V>>();
  }

  public V lookup(K key) {
    SoftReference<V> val = map.get(key);
    if (val == null) {
      return null;
    }

    V v = val.get();
    if (v == null) {
      return null;
    }

    // update position (tail of list is most recently used)
    boolean ok = fifo.remove(key);
    assert (ok);
    fifo.addLast(key);
    return v;
  }

  /**
   * update lru cache, and return evicted or old value. Returns null otherwise.
   */
  public V insert(K key, V val) {
    if (key == null || val == null) {
      return null;
    }

    V v0 = lookup(key); // updates position
    if (v0 != null) {
      // update map entry
      map.put(key, new SoftReference<V>(val));
      // (if same, nothing changes)
      return v0;
    }

    SoftReference<V> ret = null;
    // if too big, evict
    if (fifo.size() >= maxsize) {
      K kOut = fifo.removeFirst();
      ret = map.get(kOut);
      map.remove(kOut);
    }

    // install new pair
    fifo.addLast(key);
    map.put(key, new SoftReference<V>(val));
    return (ret == null) ? null : ret.get();
  }

  public void clear() {
    fifo.clear();
    map.clear();
  }
}
