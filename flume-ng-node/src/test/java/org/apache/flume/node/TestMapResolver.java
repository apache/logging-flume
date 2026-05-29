/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.node;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the MapResolver.
 */
public class TestMapResolver {

  public static final String TEST_CONST = "Apache Flume";
  private static final String TEST_PROPS = "target/test-classes/map-resolver.properties";
  private static final String NAME_VALUE = "FLUME";

  @After
  public void after() {
    System.clearProperty("lookups");
  }

  @Test
  public void testDefaultResolver() throws Exception {
    Properties props = new Properties();
    props.load(new FileInputStream(TEST_PROPS));
    System.setProperty("name", NAME_VALUE);
    Map<String, String> properties = MapResolver.resolveProperties(props);
    String name = properties.get("name");
    assertNotNull("No name property", name);
    assertEquals("Incorrect system property resolution", NAME_VALUE, name);
    String testStr = properties.get("const");
    assertNotNull("No const property", testStr);
    assertTrue("Constant was resolved", testStr.contains("${const:"));
    String version = properties.get("version");
    assertNotNull("No Java property", version);
    assertFalse("Java lookup was not resolved", version.contains("${java:"));
  }

  @Test
  public void testCustomResolver() throws Exception {
    Properties props = new Properties();
    props.load(new FileInputStream(TEST_PROPS));
    System.setProperty("name", NAME_VALUE);
    System.setProperty("lookups", "test-lookups.properties");
    Map<String, String> properties = MapResolver.resolveProperties(props);
    String name = properties.get("name");
    assertNotNull("No name property", name);
    assertEquals("Incorrect system property resolution", NAME_VALUE, name);
    String testStr = properties.get("const");
    assertNotNull("No const property", testStr);
    assertTrue("Constant was resolved", testStr.contains("${const:"));
    String version = properties.get("version");
    assertNotNull("No Java property", version);
    assertFalse("Java lookup was not resolved", version.contains("${java:"));
    String test = properties.get("test");
    assertNotNull("No Test property", version);
    assertEquals("Test lookup was not resolved", "Value", test);
  }

  @Test
  public void testOrderOfEvaluation() {
    // Tests that the evaluation order does not depend
    // on the order of Properties#propertyNames
    Properties properties = new Properties();
    properties.setProperty("a", "${b}");
    properties.setProperty("b", "OK");
    Map<String, String> resolveProperties = MapResolver.resolveProperties(properties);
    assertEquals("Incorrect order of evaluation", "OK", resolveProperties.get("a"));

    properties = new Properties();
    properties.setProperty("b", "${a}");
    properties.setProperty("a", "OK");
    resolveProperties = MapResolver.resolveProperties(properties);
    assertEquals("Incorrect order of evaluation", "OK", resolveProperties.get("b"));
  }

  @Test
  public void testDoesNotResolveRecursiveLookup() {
    // Commons Text has a self-recursion guard
    Properties props = new Properties();
    props.setProperty("a", "${a}");
    try {
        // If it does throw, it should return the definition
        assertEquals("${a}", MapResolver.resolveProperties(props).get("a"));
    } catch (IllegalStateException ignored) {
        // or it can throw
    }

    props = new Properties();
    props.setProperty("a", "${b}");
    props.setProperty("b", "${a}");
      try {
          // If it does throw, it should return the definition
          assertEquals("${b}", MapResolver.resolveProperties(props).get("a"));
      } catch (IllegalStateException ignored) {
          // or it can throw
      }
  }

}
