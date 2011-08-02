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

import junit.framework.TestCase;

@SuppressWarnings("unchecked")
public class TestState extends TestCase {
  public void testSimpleExtension() {
    State s = new State(0);
    State s2 = s.extend("a".getBytes()[0]);
    assertTrue(s2 != s && s2 != null);
    assertEquals(2, s.size());
  }

  public void testSimpleExtensionSparse() {
    State s = new State(50);
    State s2 = s.extend((byte) 3);
    assertTrue(s2 != s && s2 != null);
    assertEquals(2, s.size());
  }

  public void testSingleState() {
    State s = new State(0);
    assertEquals(1, s.size());
  }

  public void testSingleStateSparse() {
    State s = new State(50);
    assertEquals(1, s.size());
  }

  public void testExtendAll() {
    State s = new State(0);
    s.extendAll("hello world".getBytes());
    assertEquals(12, s.size());
  }

  public void testExtendAllTwiceDoesntAddMoreStates() {
    State s = new State(0);
    State s2 = s.extendAll("hello world".getBytes());
    State s3 = s.extendAll("hello world".getBytes());
    assertEquals(12, s.size());
    assertTrue(s2 == s3);
  }

  public void testExtendAllTwiceDoesntAddMoreStatesSparse() {
    State s = new State(50);
    State s2 = s.extendAll("hello world".getBytes());
    State s3 = s.extendAll("hello world".getBytes());
    assertEquals(12, s.size());
    assertTrue(s2 == s3);
  }

  public void testAddingALotOfStatesIsOk() {
    State s = new State(0);
    for (int i = 0; i < 256; i++)
      s.extend((byte) i);
    assertEquals(257, s.size());
  }

  public void testAddingALotOfStatesIsOkOnSparseRep() {
    State s = new State(50);
    for (int i = 0; i < 256; i++)
      s.extend((byte) i);
    assertEquals(257, s.size());
  }

}
