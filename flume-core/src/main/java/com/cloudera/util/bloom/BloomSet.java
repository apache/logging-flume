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

package com.cloudera.util.bloom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.google.common.base.Preconditions;

/**
 * This is an interface for a bloom set. Currently this shim wraps the
 * o.a.h.util.bloom.BloomFilter with a more generic API but uses inefficient
 * calls.
 * 
 * This is only for test code so for now. Operations of this are woefully
 * inefficient computationally and this should be eventually replaced with a
 * version that does not rely on hadoop's Writeable-tied version.
 */
public class BloomSet {

  final BloomFilter bloom;
  final static int hashType = Hash.JENKINS_HASH; // just pick a default for now.

  /**
   * Create empty BloomSet.
   * 
   * Wikipedia uses the following size and # of hashes to achieve particular
   * false positives rates.
   * 
   * m bits needed, n inserted elements, p false positive rate, k hash
   * functions, e is max false positive error rate.
   * 
   * k = (m/n) ln 2
   * 
   * m = - n ln p / (ln 2)^2
   * 
   * 1.44 log_2 (1/e) = # of bits per inserted element
   **/
  public BloomSet(int nbits, int hashes) {
    bloom = new BloomFilter(nbits, hashes, hashType);
  }

  /**
   * Copy constructor
   */
  public BloomSet(BloomSet src) {
    Preconditions.checkArgument(src != null);
    byte[] bytes = src.getBytes();
    bloom = deserialize(bytes);
  }

  /**
   * Instantiate a serialized BloomSet.
   */
  public BloomSet(byte[] serialized) {
    bloom = deserialize(serialized);
  }

  /**
   * Takes an array of bytes and deserializes it into the current BloomSet.
   */
  protected BloomFilter deserialize(byte[] serialized) {
    try {
      Preconditions.checkArgument(serialized != null);
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(
          serialized));
      BloomFilter bloom = new BloomFilter(); // empty constructor
      bloom.readFields(in);
      return bloom;
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  /**
   * Adds an int to the bloom filter.
   */
  public void addInt(int i) {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(i);
    Key k = new Key(buf.array());
    bloom.add(k);
  }

  /**
   * Get a serialized version of the BloomSet
   */
  public byte[] getBytes() {
    try {
      // serialize
      ByteArrayOutputStream bits = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bits);
      bloom.write(out);
      out.flush();
      return bits.toByteArray();
    } catch (IOException e) {
      // should never happen.
      e.printStackTrace();
      return null;
    }

  }

  @Override
  public int hashCode() {
    // TODO likely inefficient
    return Arrays.hashCode(getBytes());
  }

  @Override
  public boolean equals(Object b) {
    if (!(b instanceof BloomSet)) {
      return false;
    }

    byte[] as = getBytes();
    byte[] bs = ((BloomSet) b).getBytes();
    return Arrays.equals(as, bs);
  }

  /**
   * Applies a bitwise 'and', modifying this bloom sets. 'and'ing these actually
   * creates a new physical representation that is equivalent to anding the two
   * sets.
   */
  public void and(BloomSet b) {
    bloom.and(b.bloom);
  }

  /**
   * Rhetorically asks, does the current set contain the specified subset with
   * high probability?
   * 
   * This essentially does a 'and' and then verifies if the resulting set is
   * equal to the original.
   */
  public boolean contains(BloomSet subset) {
    // examples:
    // super sub => (sub & super) ^ sub == 0
    // 1111 0011 => true
    // 1100 1100 => true
    // 1100 1000 => true
    // 1100 0000 => true
    // 1100 0010 => false
    // serialize the bloom filter, and send it on close.
    BloomSet subClone = new BloomSet(subset);

    // if subset
    subClone.and(this);
    return subClone.equals(subset);
  }
}
