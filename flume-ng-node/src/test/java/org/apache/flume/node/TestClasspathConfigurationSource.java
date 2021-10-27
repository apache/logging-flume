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

import java.net.URI;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that files can be loaded from the Classpath.
 */
public class TestClasspathConfigurationSource {

  @Test
  public void testClasspath() throws Exception {
    URI confFile = new URI("classpath:///flume-conf.properties");
    ConfigurationSource source = new ClasspathConfigurationSource(confFile);
    Assert.assertNotNull("No configuration returned", source);
    Properties props = new Properties();
    props.load(source.getInputStream());
    String value = props.getProperty("host1.sources");
    Assert.assertNotNull("Missing key", value);
  }

  @Test
  public void testOddClasspath() throws Exception {
    URI confFile = new URI("classpath:/flume-conf.properties");
    ConfigurationSource source = new ClasspathConfigurationSource(confFile);
    Assert.assertNotNull("No configuration returned", source);
    Properties props = new Properties();
    props.load(source.getInputStream());
    String value = props.getProperty("host1.sources");
    Assert.assertNotNull("Missing key", value);
  }

  @Test
  public void testImproperClasspath() throws Exception {
    URI confFile = new URI("classpath://flume-conf.properties");
    ConfigurationSource source = new ClasspathConfigurationSource(confFile);
    Assert.assertNotNull("No configuration returned", source);
    Properties props = new Properties();
    props.load(source.getInputStream());
    String value = props.getProperty("host1.sources");
    Assert.assertNotNull("Missing key", value);
  }

  @Test
  public void testShorthandClasspath() throws Exception {
    URI confFile = new URI("classpath:flume-conf.properties");
    ConfigurationSource source = new ClasspathConfigurationSource(confFile);
    Assert.assertNotNull("No configuration returned", source);
    Properties props = new Properties();
    props.load(source.getInputStream());
    String value = props.getProperty("host1.sources");
    Assert.assertNotNull("Missing key", value);
  }
}
