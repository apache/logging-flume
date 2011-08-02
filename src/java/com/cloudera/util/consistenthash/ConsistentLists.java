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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consistent hash lets you place bins and map values to bins. However, how does
 * one get a bin and find out what values are there? For that we keep track of
 * the values put into the table, and update them when the bins change. This is
 * notably inefficient.
 * 
 * This structure provides a facility to add and remove values and to add and
 * remove keys/bins.
 * 
 * Keys of type T are the bins in the consistent hash (and have many nodes).
 * Values of type V are the values that get assigned to a bin.
 * 
 * One can register a MoveHandler that has callbacks for when values are moved
 * from one bin to another. This eventually allows for notification of
 * incremental updates.
 * 
 * This is not thread safe.
 */
public class ConsistentLists<T, V> {
  // consistent hash that assigns values of type V to key bins of type K
  final ConsistentHash<T> binHash;

  // buckets the values live in
  final Map<T, List<V>> valueLists;

  final List<MoveHandler<T, V>> listeners = new ArrayList<MoveHandler<T, V>>();

  public ConsistentLists(int replicationFactor) {
    binHash = new ConsistentHash<T>(replicationFactor, new ArrayList<T>());
    valueLists = new HashMap<T, List<V>>();
  }

  public Map<T, List<V>> getValueLists() {
    return Collections.unmodifiableMap(valueLists);
  }

  public void addMoveListener(MoveHandler<T, V> l) {
    listeners.add(l);
  }

  public void removeMoveListener(MoveHandler<T, V> l) {
    listeners.remove(l);
  }

  private void fireMoved(T fromBin, T toBin, List<V> vals) {
    if (vals == null || vals.isEmpty())
      return;
    for (MoveHandler<T, V> l : listeners) {
      l.moved(fromBin, toBin, vals);
    }
  }

  /**
   * This iterates through each bin and fires an event with all of the values
   * associated with it. This corresponds to a "rebuild" where we bulk load a
   * mappings and then send notification in bulk instead of incrementally.
   */
  public void rebuild() {
    for (Map.Entry<T, List<V>> ent : valueLists.entrySet()) {
      List<V> vals = ent.getValue();
      T fromBin = ent.getKey();
      fireRebuild(fromBin, vals);
    }
  }

  private void fireRebuild(T fromBin, List<V> allVals) {
    for (MoveHandler<T, V> l : listeners) {
      l.rebuild(fromBin, allVals);
    }
  }

  /**
   * figure out which node to assign the value, then add it.
   */
  public void addValue(V exp) {
    T w = binHash.getBinFor(exp);
    List<V> valList = valueLists.get(w);
    if (valList == null) {
      valList = new ArrayList<V>();
      valueLists.put(w, valList);
    }

    valList.add(exp);

    // send alerts
    List<V> vals = new ArrayList<V>();
    vals.add(exp);
    fireMoved(null, w, vals);
  }

  /**
   * figure out which node is supposed to have the value and remove it.
   */
  public void removeValue(V exp) {
    T w = binHash.getBinFor(exp);
    List<V> valList = valueLists.get(w);
    if (valList == null) {
      throw new IllegalStateException(
          "Weird! cannot remove item that doesn't exist: " + exp);
    }
    valList.remove(exp);

    // send alert
    List<V> vals = new ArrayList<V>();
    vals.add(exp);
    fireMoved(w, null, vals);

  }

  /**
   * Adding a new bin to the set. Need to make sure things are consistent.
   */
  public void addBin(T newBin) {
    binHash.addBin(newBin);

    // After we update the bins, values in the value lists may be in the wrong
    // place.

    // now I need to march through all of the other bins to figure out which
    // ones moved and move them.
    List<V> newBinValues = new ArrayList<V>();
    for (Map.Entry<T, List<V>> ent : valueLists.entrySet()) {
      T bin = ent.getKey();
      List<V> valList = ent.getValue();
      List<V> movingList = new ArrayList<V>();
      for (V v : valList) {
        T curBin = binHash.getBinFor(v);

        // didn't move, do nothing
        if (curBin == newBin) {
          movingList.add(v);

        } else if (bin != curBin) {
          // WTF? It moved to some unexpected node. BUG!
          throw new RuntimeException("wtf;  value moved to a random bin!");
        }
      }
      // replace previous bin with new accurate bin
      newBinValues.addAll(movingList);
      valList.removeAll(movingList);
      fireMoved(bin, newBin, movingList);
    }
    // and now add the bins for the new worker
    valueLists.put(newBin, newBinValues);
  }

  /**
   * Removes a bin and adjust the value lists by moving values to their new
   * assignment.
   */
  public void removeBin(T bin) {
    List<V> moving = valueLists.get(bin);
    binHash.removeBin(bin);

    // for each possible destination bin, gather the moving values and fire an
    // alert.
    for (Map.Entry<T, List<V>> ent : valueLists.entrySet()) {
      T dstBin = ent.getKey();
      List<V> oldList = ent.getValue();
      List<V> movedList = new ArrayList<V>();
      // go through the list of expression from the one getting removed
      for (V v : moving) {
        T newBin = binHash.getBinFor(v);
        if (newBin.equals(dstBin)) {
          // the value has been moved from
          movedList.add(v);
        }
      }
      oldList.addAll(movedList);
      fireMoved(bin, dstBin, movedList);
    }
    // everyone should be assigned

    // remove the list
    valueLists.remove(bin);
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Map.Entry<T, List<V>> ent : valueLists.entrySet()) {
      T bin = ent.getKey();
      List<V> vs = ent.getValue();
      buf.append(bin);
      buf.append(" => ");
      buf.append(vs);
      buf.append("\n");
    }
    return buf.toString();
  }

  public List<T> keys() {
    List<T> ks = new ArrayList<T>();
    ks.addAll(valueLists.keySet());
    return ks;
  }

}
