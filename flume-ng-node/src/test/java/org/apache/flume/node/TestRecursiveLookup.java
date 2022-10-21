/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.File;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import com.google.common.collect.Lists;

import junit.framework.Assert;

public class TestRecursiveLookup {
  private static final File TESTFILE = new File(
      TestRecursiveLookup.class.getClassLoader()
          .getResource("flume-conf-with-recursiveLookup.properties").getFile());
  private static final String BIND = "192.168.11.101";

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
  private UriConfigurationProvider provider;

  @Before
  public void setUp() throws Exception {
    System.setProperty("env", "DEV");
    List<ConfigurationSource> sourceList =
        Lists.newArrayList(new FileConfigurationSource(TESTFILE.toURI()));
    provider = new UriConfigurationProvider("a1", sourceList, null,
        null, 0);
  }

  @Test
  public void getProperty() throws Exception {

    Assert.assertEquals(BIND, provider.getFlumeConfiguration()
        .getConfigurationFor("a1")
        .getSourceContext().get("r1").getParameters().get("bind"));
  }
}
