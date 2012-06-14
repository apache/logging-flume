/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.UUID;

/**
 * Utility class for users to generate their own keys. Any key can be used,
 * this is just a utility that provides a set of simple keys.
 *
 *
 */
public class SimpleRowKeyGenerator {

  public static byte[] getUUIDKey(String prefix)
      throws UnsupportedEncodingException{
    return (prefix + UUID.randomUUID().toString()).getBytes("UTF8");
  }

  public static byte[] getRandomKey(String prefix)
      throws UnsupportedEncodingException{
    return (prefix + String.valueOf(new Random().nextLong())).getBytes("UTF8");
  }
  public static byte[] getTimestampKey(String prefix)
      throws UnsupportedEncodingException {
    return (prefix + String.valueOf(
        System.currentTimeMillis())).getBytes("UTF8");
  }
  public static byte[] getNanoTimestampKey(String prefix)
      throws UnsupportedEncodingException{
    return (prefix + String.valueOf(
        System.nanoTime())).getBytes("UTF8");
  }
}
