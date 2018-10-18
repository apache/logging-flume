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
package org.apache.flume.util;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

public abstract class AbstractSSLUtilListTest extends AbstractSSLUtilTest {
  @Parameters
  public static Collection<?> data() {
    return Arrays.asList(new Object[][]{
      // system property value, environment variable value, expected value
      { null, null, null },
      { "sysprop", null, "sysprop" },
      { "sysprop,sysprop", null, "sysprop sysprop" },
      { null, "envvar", "envvar" },
      { null, "envvar,envvar", "envvar envvar" },
      { "sysprop", "envvar", "sysprop" },
      { "sysprop,sysprop", "envvar,envvar", "sysprop sysprop" }
    });
  }

  protected AbstractSSLUtilListTest(String sysPropValue, String envVarValue, String expectedValue) {
    super(sysPropValue, envVarValue, expectedValue);
  }
}
