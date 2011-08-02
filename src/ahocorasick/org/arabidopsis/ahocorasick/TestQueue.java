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
public class TestQueue extends TestCase {
    public void testSimple() {
	Queue q = new Queue();
	State s1 = new State(0);
	State s2 = new State(0);
	State s3 = new State(0);
	State s4 = new State(0);
	State s5 = new State(0);
	assertTrue(q.isEmpty());
	q.add(s1);
	assertFalse(q.isEmpty());
	assertEquals(s1, q.pop());

	q.add(s2);
	q.add(s3);
	assertEquals(s2, q.pop());
	q.add(s4);
	q.add(s5);
	assertEquals(s3, q.pop());
	assertEquals(s4, q.pop());
	assertEquals(s5, q.pop());
	assertTrue(q.isEmpty());
    }
}
