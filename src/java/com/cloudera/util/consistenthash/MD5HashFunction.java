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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses MD5 as a hash generator. This version actually takes a hashs of the
 * toString String.
 */
public class MD5HashFunction implements HashFunction {
  static final Logger LOG = LoggerFactory.getLogger(MD5HashFunction.class);

  MessageDigest digest;

  public MD5HashFunction() {
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("MD5 algorithm doesn't exist?", e);
      throw new IllegalArgumentException("This should never happen");
    }
  }

  public int hash(Object s) {
    digest.reset();
    byte[] hash = digest.digest(s.toString().getBytes());

    // HACK just take the first 4 digits and make it an integer.
    // apparently this is what other algorithms use to turn it into an int
    // value.

    // http://github.com/dustin/java-memcached-client/blob/9b2b4be73ee4a74bf6d0cf47f89c33753a5b5329/src/main/java/net/spy/memcached/HashAlgorithm.java
    int h0 = (hash[0] & 0xFF);
    int h1 = (hash[1] & 0xFF) << 8;
    int h2 = (hash[2] & 0xFF) << 16;
    int h3 = (hash[3] & 0xFF) << 24;

    int val = h0 + h1 + h2 + h3;
    return val;
  }

}
