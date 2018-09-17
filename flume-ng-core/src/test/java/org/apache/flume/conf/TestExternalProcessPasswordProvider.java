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
package org.apache.flume.conf;

import com.google.common.collect.ImmutableMap;
import org.apache.flume.Context;
import org.junit.Assert;
import org.junit.Test;

public class TestExternalProcessPasswordProvider {

  @Test
  public void testValidCommand() {
    Context context = new Context(ImmutableMap.of(
        "password.command", "echo -n applepie"));
    ExternalProcessPasswordProvider passwordProvider = new ExternalProcessPasswordProvider();
    Assert.assertEquals("applepie", passwordProvider.getPassword(context, "password"));
  }

  @Test
  public void testUTF8() {
    Context context = new Context(ImmutableMap.of(
        "password.command", "echo -n applepieéáű",
        "password.charset", "utf-8"));
    ExternalProcessPasswordProvider passwordProvider = new ExternalProcessPasswordProvider();
    Assert.assertEquals("applepieéáű", passwordProvider.getPassword(context, "password"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoCommand() {
    Context context = new Context();
    ExternalProcessPasswordProvider passwordProvider = new ExternalProcessPasswordProvider();
    passwordProvider.getPassword(context, "password");
  }

}
