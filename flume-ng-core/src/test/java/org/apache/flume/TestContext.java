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

package org.apache.flume;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestContext {

  private Context context;

  @Before
  public void setUp() {
    context = new Context();
  }

  @Test
  public void testPutGet() {
    assertEquals("Context is empty", 0, context.getParameters().size());

    context.put("test", "value");
    assertEquals("value", context.getString("test"));
    context.clear();
    assertNull(context.getString("test"));
    assertEquals("value", context.getString("test", "value"));

    context.put("test", "true");
    assertEquals(new Boolean(true), context.getBoolean("test"));
    context.clear();
    assertNull(context.getBoolean("test"));
    assertEquals(new Boolean(true), context.getBoolean("test", true));

    context.put("test", "1");
    assertEquals(new Integer(1), context.getInteger("test"));
    context.clear();
    assertNull(context.getInteger("test"));
    assertEquals(new Integer(1), context.getInteger("test", 1));

    context.put("test", String.valueOf(Long.MAX_VALUE));
    assertEquals(new Long(Long.MAX_VALUE), context.getLong("test"));
    context.clear();
    assertNull(context.getLong("test"));
    assertEquals(new Long(Long.MAX_VALUE), context.getLong("test", Long.MAX_VALUE));

  }

  @Test
  public void testSubProperties() {
    context.put("my.key", "1");
    context.put("otherKey", "otherValue");
    assertEquals(ImmutableMap.of("key", "1"), context.getSubProperties("my."));

  }

  @Test
  public void testClear() {
    context.put("test", "1");
    context.clear();
    assertNull(context.getInteger("test"));
  }

  @Test
  public void testPutAll() {
    context.putAll(ImmutableMap.of("test", "1"));
    assertEquals("1", context.getString("test"));
  }
}
