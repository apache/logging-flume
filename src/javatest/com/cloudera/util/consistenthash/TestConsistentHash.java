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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.agent.AgentSink;

/**
 * This is a test harness for the consistent hash implementation.
 */
public class TestConsistentHash {
  final static Logger LOG = Logger.getLogger(AgentSink.class.getName());

  // These are the bins values can go into
  List<String> machines = Arrays.asList("machine A", "machine B", "machine C",
      "machine D", "machine E");

  /**
   * This tests that the majority of values do not move after adding another
   * bin, and also that the original partitioning is restored when the original
   * set of bins is restored.
   */
  @Test
  public void testConsistentHash() {
    int replicas = 100;
    ConsistentHash<String> hash = new ConsistentHash<String>(replicas, machines);

    List<String> orig = new ArrayList<String>(20);
    StringBuilder sb = new StringBuilder();
    sb.append("Before: ");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getBinFor(s);
      sb.append(bin + ", ");
      orig.add(bin);
    }
    LOG.info(sb.toString());

    int diffs = 0;
    sb = new StringBuilder();
    sb.append("after adding a machine: ");
    hash.addBin("machine F");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getBinFor(s);
      sb.append(bin + ", ");
      if (!orig.get(i).equals(bin)) {
        diffs++;
      }
    }
    LOG.info(sb.toString());
    // ideally there should only be about 4 that have changed.
    LOG.info("Adding one caused " + diffs + " out of 20 to change");

    // we should have the original setup back again.
    LOG.info("after adding a machine: ");
    hash.removeBin("machine F");
    sb = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getBinFor(s);
      sb.append(bin + ", ");
      assertEquals(orig.get(i), bin);
    }
    LOG.info(sb.toString());

  }

  /**
   * This tests getting N successive bins from the consistent hash for each
   * value. These will be the failovers. getNBinsFor can have duplicates.
   */
  @Test
  public void testConsistentHashN() {
    int replicas = 100;
    ConsistentHash<String> hash = new ConsistentHash<String>(replicas, machines);

    List<String> orig = new ArrayList<String>(20);
    StringBuilder sb = new StringBuilder();
    LOG.info("Before: ");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      List<String> l = hash.getNBinsFor(s, 3);
      String bin = l.toString();
      sb.append(bin + ", ");
      // save the first of the n.
      orig.add(l.get(0).toString());
    }
    LOG.info(sb.toString());

    sb = new StringBuilder();
    int diffs = 0;
    LOG.info("after adding a machine: ");
    hash.addBin("machine F");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      List<String> l = hash.getNBinsFor(s, 3);
      String bin = l.toString();
      sb.append(bin + ", ");
      // compare the updated with the first.
      if (!orig.get(i).equals(l.get(0))) {
        diffs++;
      }
    }
    LOG.info(sb.toString());
    // ideally there should only be about 4 that have changed.
    LOG.info("Adding one caused " + diffs + " out of 20 to change");

    // we should have the original setup back again.
    LOG.info("after adding a machine: ");
    sb = new StringBuilder();
    hash.removeBin("machine F");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getBinFor(s);
      sb.append(bin + ", ");
      assertEquals(orig.get(i), bin);
    }
    LOG.info(sb.toString());

  }

  /**
   * This tests getting N successive bins from the consistent hash for each
   * value. These will are the failovers. getNUniqBinsFor does not allow
   * duplicats in the returned list.
   */
  @Test
  public void testConsistentHashNUniq() {
    int replicas = 100;
    ConsistentHash<String> hash = new ConsistentHash<String>(replicas, machines);
    StringBuilder sb = new StringBuilder();

    List<String> orig = new ArrayList<String>(20);
    LOG.info("Before: ");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      List<String> l = hash.getNUniqueBinsFor(s, 3);
      String bin = l.toString();
      sb.append(bin + ", ");
      orig.add(l.get(0).toString());

      // test uniqueness
      Set<String> set = new HashSet<String>(l);
      assertEquals(3, set.size());
    }
    LOG.info(sb.toString());

    int diffs = 0;
    LOG.info("after adding a machine: ");
    hash.addBin("machine F");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getNUniqueBinsFor(s, 3).toString();
      sb.append(bin + ", ");
      if (!orig.get(i).equals(bin)) {
        diffs++;
      }
    }
    LOG.info(sb.toString());
    // ideally there should only be about 4 that have changed.
    LOG.info("Adding one caused " + diffs + " out of 20 to change");

    sb = new StringBuilder();

    // we should have the original setup back again.
    LOG.info("after adding a machine: ");
    hash.removeBin("machine F");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      String bin = hash.getBinFor(s);
      sb.append(bin + ", ");
      assertEquals(orig.get(i), bin);
    }
    LOG.info(sb.toString());

  }

  /**
   * This tests getting N successive bins from the consistent hash for each
   * value. Here we have fewer buckets than requested failovers -- this call
   * succeeds but returns fewer than the desired 3 bin. This allows the
   * algorithm to still succeed if we don't have many bins.
   */
  @Test
  public void testConsistentHashNUniqTooFew() {
    int replicas = 100;
    ConsistentHash<String> hash = new ConsistentHash<String>(replicas, Arrays
        .asList("single machine"));
    StringBuilder sb = new StringBuilder();

    List<String> orig = new ArrayList<String>(20);
    LOG.info("Before: ");
    for (int i = 0; i < 20; i++) {
      String s = "this is a the key " + i;
      List<String> l = hash.getNUniqueBinsFor(s, 3);
      String bin = l.toString();
      sb.append(bin + ", ");
      orig.add(l.get(0).toString());

      // test uniqueness
      Set<String> set = new HashSet<String>(l);
      assertEquals(1, set.size());
    }
    LOG.info(sb.toString());

  }

  /**
   * This tests to make sure that the ConsistentLists work, and that values are
   * reasonably distributed amongst the bins.
   */
  @Test
  public void testConsistentLists() {
    int replicas = 100;
    int values = 50;
    ConsistentLists<String, String> lists = new ConsistentLists<String, String>(
        replicas);

    lists.addMoveListener(new MoveHandler<String, String>() {

      @Override
      public void moved(String from, String to, List<String> values) {
        LOG.info(String.format("from %s to %s : values %s", from, to, values));
      }

      @Override
      public void rebuild(String key, List<String> allVals) {
        LOG.info("Rebuild: " + key);
      }
    });

    for (String m : machines) {
      lists.addBin(m);
    }

    LOG.info("Before: ");
    for (int i = 0; i < values; i++) {
      lists.addValue(String.format("value%04d", i));
    }

    int sum = 0;
    int buckets = 0;
    for (List<String> vs : lists.getValueLists().values()) {
      sum += vs.size();
      buckets++;
    }
    assertEquals(buckets, 5);
    assertEquals(sum, values);

    LOG.info("adding a machine F: ");
    lists.addBin("machine F");
    LOG.info("Lists values:\n" + lists);

    sum = 0;
    buckets = 0;
    for (List<String> vs : lists.getValueLists().values()) {
      sum += vs.size();
      buckets++;
    }
    assertEquals(buckets, 6);
    assertEquals(sum, values);

    LOG.info("removing machine B: ");
    lists.removeBin("machine B");
    LOG.info("Lists values:\n" + lists);

    sum = 0;
    buckets = 0;
    for (List<String> vs : lists.getValueLists().values()) {
      sum += vs.size();
      buckets++;
    }
    assertEquals(buckets, 5);
    assertEquals(sum, values);

    LOG.info("removing machine D: ");
    lists.removeBin("machine D");
    LOG.info("Lists values:\n" + lists);
    sum = 0;
    buckets = 0;

    for (List<String> vs : lists.getValueLists().values()) {
      int sz = vs.size();
      sum += sz;
      buckets++;
    }
    assertEquals(sum, values);
    assertEquals(buckets, 4);

    Map<String, List<String>> vls = lists.getValueLists();
    assertEquals(13, vls.get("machine E").size());
    assertEquals(18, vls.get("machine F").size());
    assertEquals(8, vls.get("machine C").size());
    assertEquals(11, vls.get("machine A").size());
  }
}
