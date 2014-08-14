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

package org.apache.flume.test.agent;

import org.apache.flume.test.util.SyslogAgent;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TestSyslogSource {
  private static final Logger LOGGER = Logger.getLogger(TestSyslogSource.class);

  private SyslogAgent agent;
  private SyslogAgent.SyslogSourceType sourceType;

  public TestSyslogSource(SyslogAgent.SyslogSourceType sourceType) {
    this.sourceType = sourceType;
  }

  @Parameterized.Parameters
  public static Collection syslogSourceTypes() {
    List<Object[]> sourceTypes = new ArrayList<Object[]>();
    for (SyslogAgent.SyslogSourceType sourceType : SyslogAgent.SyslogSourceType.values()) {
      sourceTypes.add(new Object[]{sourceType});
    }
    return sourceTypes;
  }

  @Before
  public void setUp() throws Exception {
    agent = new SyslogAgent();
    agent.configure(sourceType);
  }

  @After
  public void tearDown() throws Exception {
    if (agent != null) {
      agent.stop();
      agent = null;
    }
  }

  @Test
  public void testKeepFields() throws Exception {
    LOGGER.debug("testKeepFields() started.");

    agent.start("all");
    agent.runKeepFieldsTest();

    LOGGER.debug("testKeepFields() ended.");
  }

  @Test
  public void testRemoveFields() throws Exception {
    LOGGER.debug("testRemoveFields() started.");

    agent.start("none");
    agent.runKeepFieldsTest();

    LOGGER.debug("testRemoveFields() ended.");
  }

  @Test
  public void testKeepTimestampAndHostname() throws Exception {
    LOGGER.debug("testKeepTimestampAndHostname() started.");

    agent.start("timestamp hostname");
    agent.runKeepFieldsTest();

    LOGGER.debug("testKeepTimestampAndHostname() ended.");
  }
}
