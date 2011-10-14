/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel.jdbc;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.jdbc.impl.JdbcChannelProviderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJdbcChannelProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestJdbcChannelProvider.class);

  private Properties derbyProps = new Properties();
  private File derbyDbDir;
  private JdbcChannelProviderImpl provider;

  @Before
  public void setUp() throws IOException {
    derbyProps.clear();
    derbyProps.put(ConfigurationConstants.CONFIG_CREATE_SCHEMA, "true");
    derbyProps.put(ConfigurationConstants.CONFIG_DATABASE_TYPE, "DERBY");
    derbyProps.put(ConfigurationConstants.CONFIG_JDBC_DRIVER_CLASS,
        "org.apache.derby.jdbc.EmbeddedDriver");

    derbyProps.put(ConfigurationConstants.CONFIG_PASSWORD, "");
    derbyProps.put(ConfigurationConstants.CONFIG_USERNAME, "sa");

    File tmpDir = new File("target/test");
    tmpDir.mkdirs();

    // Use a temp file to create a temporary directory
    File tempFile = File.createTempFile("temp", "_db", tmpDir);
    String absFileName = tempFile.getCanonicalPath();
    tempFile.delete();

    derbyDbDir = new File(absFileName + "_dir");

    if (!derbyDbDir.exists()) {
      derbyDbDir.mkdirs();
    }

    derbyProps.put(ConfigurationConstants.CONFIG_URL,
        "jdbc:derby:" + derbyDbDir.getCanonicalPath() + "/db;create=true");

    LOGGER.info("Derby Properties: " + derbyProps);
  }

  @Test
  public void testDerbySetup() {
    provider = new JdbcChannelProviderImpl();

    provider.initialize(derbyProps);

    Transaction tx1 = provider.getTransaction();
    tx1.begin();

    Transaction tx2 = provider.getTransaction();

    Assert.assertSame(tx1, tx2);
    tx2.begin();
    tx2.close();
    tx1.close();

    Transaction tx3 = provider.getTransaction();
    Assert.assertNotSame(tx1, tx3);

    tx3.begin();
    tx3.close();

    provider.close();
    provider = null;
  }

  @Test
  public void testPeristingEvents() {
    provider = new JdbcChannelProviderImpl();
    provider.initialize(derbyProps);

    int nameLimit = ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD;
    int th = ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD;

    byte[] s1 = MockEventUtils.generatePayload(th - 1);
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put(MockEventUtils.generateHeaderString(1), "one");
    m1.put(MockEventUtils.generateHeaderString(2), "twotwo");
    m1.put(MockEventUtils.generateHeaderString(3), "three");
    m1.put(MockEventUtils.generateHeaderString(100), "ahundred");
    m1.put(MockEventUtils.generateHeaderString(nameLimit - 21), "w");
    m1.put(MockEventUtils.generateHeaderString(nameLimit - 2), "x");
    m1.put(MockEventUtils.generateHeaderString(nameLimit - 1), "y");
    m1.put(MockEventUtils.generateHeaderString(nameLimit), "z");
    m1.put(MockEventUtils.generateHeaderString(nameLimit + 1), "a");
    m1.put(MockEventUtils.generateHeaderString(nameLimit + 2), "b");
    m1.put(MockEventUtils.generateHeaderString(nameLimit + 21), "c");

    Event event = new MockEvent(s1, m1);

    provider.persistEvent("test", event);

    provider.close();
    provider = null;
  }

  @After
  public void tearDown() throws IOException {
    if (provider != null) {
      try {
        provider.close();
      } catch (Exception ex) {
        LOGGER.error("Unable to close provider", ex);
      }
    }
    provider = null;
  }
}
