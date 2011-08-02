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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Quick and dirty code: measures the amount of time it takes to construct an
 * AhoCorasick tree out of all the words in <tt>/usr/share/dict/words</tt>.
 */

@SuppressWarnings("unchecked")
public class TimeTrial {
  static public void main(String[] args) throws IOException {
    long startTime = System.currentTimeMillis();
    AhoCorasick tree = new AhoCorasick();
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream("/usr/share/dict/words")));
    String line;
    while ((line = reader.readLine()) != null) {
      tree.add(line.getBytes(), null);
    }
    tree.prepare();
    long endTime = System.currentTimeMillis();
    System.out.println("endTime - startTime = " + (endTime - startTime)
        + " milliseconds");
  }
}
