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
package org.apache.flume.channel.file;

import junit.framework.Assert;

import org.junit.Test;

public class TestFlumeEventPointer {

  @Test
  public void testGetter() {
    FlumeEventPointer pointer = new FlumeEventPointer(1, 1);
    Assert.assertEquals(1, pointer.getFileID());
    Assert.assertEquals(1, pointer.getOffset());
  }
  @Test
  public void testEquals() {
    FlumeEventPointer pointerA = new FlumeEventPointer(1, 1);
    FlumeEventPointer pointerB = new FlumeEventPointer(1, 1);
    Assert.assertEquals(pointerA, pointerB);
    Assert.assertEquals(pointerB, pointerA);
    pointerA = new FlumeEventPointer(1, 1);
    pointerB = new FlumeEventPointer(2, 2);
    Assert.assertFalse(pointerA.equals(pointerB));
    Assert.assertFalse(pointerB.equals(pointerA));
  }
  @Test
  public void testHashCode() {
    FlumeEventPointer pointerA = new FlumeEventPointer(1, 1);
    FlumeEventPointer pointerB = new FlumeEventPointer(1, 1);
    Assert.assertEquals(pointerA.hashCode(), pointerB.hashCode());
    pointerA = new FlumeEventPointer(1, 1);
    pointerB = new FlumeEventPointer(2, 2);
    Assert.assertFalse(pointerA.hashCode() == pointerB.hashCode());
  }

  @Test
  public void testPack() {
    FlumeEventPointer pointerA = new FlumeEventPointer(1, 1);
    FlumeEventPointer pointerB = new FlumeEventPointer(1, 2);
    Assert.assertEquals(4294967297L, pointerA.toLong());
    Assert.assertEquals(4294967298L, pointerB.toLong());
    Assert.assertEquals(pointerA, FlumeEventPointer.fromLong(pointerA.toLong()));
    Assert.assertEquals(pointerB, FlumeEventPointer.fromLong(pointerB.toLong()));
  }
}
