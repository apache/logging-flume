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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestContext {

  private Context context;

  @Before
  public void setUp() {
    context = new Context();
  }

  @Test
  public void testPutGet() {
    Assert.assertEquals("Context is empty", 0, context.getParameters().size());

    context.put("test", "test");

    Assert.assertEquals("Context contains test value", "test",
        context.get("test", String.class));
  }

}
