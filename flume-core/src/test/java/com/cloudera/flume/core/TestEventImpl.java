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
package com.cloudera.flume.core;

import static org.junit.Assert.*;

import com.cloudera.flume.conf.FlumeConfiguration;

import org.junit.Test;

public class TestEventImpl {

  /**
   * Test that creating a too-big event fails with the right exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testMaxSize() {
    long maxSize = FlumeConfiguration.get().getEventMaxSizeBytes();
    new EventImpl(new byte[(int) (maxSize + 1)]);
  }

  @Test
  public void testEvilReplacement() {
    // '$' in the replacement strings are evil. If the $ has a number following
    // it replaces with a regex group. If it $ is followed by a char, it emits a
    // "IllegalArgumentException: Illegal group reference"
    Event e = new EventImpl("$evil".getBytes());
    String after = e.escapeString("this is the body: %{body}");
    assertEquals("this is the body: $evil", after);
  }

  @Test
  public void testEvilReplacement2() {
    // '\' in the replacement strings are evil. They are omitted if not escaped.
    Event e = new EventImpl("\\evil".getBytes());
    String after = e.escapeString("this is the body: %{body}");
    assertEquals("this is the body: \\evil", after);
  }

  @Test
  public void testEvilCompound() {
    Event e2 = new EventImpl("\\$\\evil".getBytes());
    String after = e2.escapeString("this is the body: %{body}");
    assertEquals("this is the body: \\$\\evil", after);
  }

}
