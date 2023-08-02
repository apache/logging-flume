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

}
