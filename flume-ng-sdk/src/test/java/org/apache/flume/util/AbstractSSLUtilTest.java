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

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@RunWith(Parameterized.class)
public abstract class AbstractSSLUtilTest {

  @Parameters
  public static Collection<?> data() {
    return Arrays.asList(new Object[][]{
        // system property value, environment variable value, expected value
        { null, null, null },
        { "sysprop", null, "sysprop" },
        { null, "envvar", "envvar" },
        { "sysprop", "envvar", "sysprop" }
    });
  }

  protected String sysPropValue;
  protected String envVarValue;
  protected String expectedValue;

  protected AbstractSSLUtilTest(String sysPropValue, String envVarValue, String expectedValue) {
    this.sysPropValue = sysPropValue;
    this.envVarValue = envVarValue;
    this.expectedValue = expectedValue;
  }

  protected abstract String getSysPropName();

  protected abstract String getEnvVarName();

  @Before
  public void setUp() {
    setSysProp(getSysPropName(), sysPropValue);
    setEnvVar(getEnvVarName(), envVarValue);
  }

  @After
  public void tearDown() {
    setSysProp(getSysPropName(), null);
    setEnvVar(getEnvVarName(), null);
  }

  private static void setSysProp(String name, String value) {
    if (value != null) {
      System.setProperty(name, value);
    } else {
      System.clearProperty(name);
    }
  }

  private static void setEnvVar(String name, String value) {
    try {
      injectEnvironmentVariable(name, value);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Test setup  failed.", e);
    }
  }

  // based on https://dzone.com/articles/how-to-change-environment-variables-in-java
  private static void injectEnvironmentVariable(String key, String value)
      throws ReflectiveOperationException {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
    Field unmodifiableMapField = getAccessibleField(processEnvironment,
        "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);
    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    if (value != null) {
      map.put(key, value);
    } else {
      map.remove(key);
    }
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {
    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    if (value != null) {
      ((Map<String, String>) obj).put(key, value);
    } else {
      ((Map<String, String>) obj).remove(key);
    }
  }

}
