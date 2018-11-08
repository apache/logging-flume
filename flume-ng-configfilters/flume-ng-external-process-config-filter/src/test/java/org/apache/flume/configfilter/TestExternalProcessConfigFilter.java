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
package org.apache.flume.configfilter;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestExternalProcessConfigFilter {

  public static final String MY_PASSWORD_KEY = "my_password_key";
  public static final String MY_PASSWORD_KEY_2 = "my_password_key2";
  public static final String FILTERED = "filtered";
  public static final String FILTERED_2 = "filtered2";
  private ExternalProcessConfigFilter configFilter;

  @Before
  public void setUp() {
    configFilter = new ExternalProcessConfigFilter();
  }

  @Test
  public void filterOk() {
    String file = Thread.currentThread().getContextClassLoader()
        .getResource("test.sh").getFile();

    File testExecutable = new File(file);
    testExecutable.setExecutable(true);

    HashMap<String, String> configuration = new HashMap<>();
    configuration.put("command", file);
    configFilter.initializeWithConfiguration(configuration);
    assertEquals(FILTERED, configFilter.filter(MY_PASSWORD_KEY));
    assertEquals(FILTERED_2, configFilter.filter(MY_PASSWORD_KEY_2));
  }

  @Test
  public void filterError() {
    String file = Thread.currentThread().getContextClassLoader()
        .getResource("test_error.sh").getFile();

    File testExecutable = new File(file);
    testExecutable.setExecutable(true);

    HashMap<String, String> configuration = new HashMap<>();
    configuration.put("command", file);
    configFilter.initializeWithConfiguration(configuration);
    assertNull(configFilter.filter(MY_PASSWORD_KEY));
  }
}