/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master.availability;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.CompositeSink;

/**
 * This tests the failover chain manager's generated code.
 */
public class TestFailChainManager {

  static final Logger LOG = LoggerFactory.getLogger(TestFailChainManager.class);

  String[] collectors =
      { "collector 1", "collector 2", "collector 3", "collector 4",
          "collector 5" };

  @Test
  public void testConsistentHashAvailMan() throws FlumeSpecException,
      IOException {
    FailoverChainManager am = new ConsistentHashFailoverChainManager(3);

    for (String c : collectors) {
      am.addCollector(c);
    }

    String agent1 =
        am.getFailChainSinkSpec("agent1", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent1);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 2\",\"collector 3\",\"collector 1\")",
        agent1);
    CompositeSink snk1 = new CompositeSink(new Context(), agent1);
    snk1.open();
    snk1.close();

    String agent2 =
        am.getFailChainSinkSpec("agent2", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent2);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 1\",\"collector 4\",\"collector 5\")",
        agent2);
    CompositeSink snk2 = new CompositeSink(new Context(), agent2);
    snk2.open();
    snk2.close();

    String agent3 =
        am.getFailChainSinkSpec("agent3", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent3);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 1\",\"collector 2\",\"collector 5\")",
        agent3);
    CompositeSink snk3 = new CompositeSink(new Context(), agent3);
    snk3.open();
    snk3.close();

  }

  /**
   * The specs generated will be deterministic because it the seeds are based
   * off a hash of the agent name.
   */
  @Test
  public void testRandomAvailMan() throws FlumeSpecException, IOException {
    FailoverChainManager am = new RandomFailoverChainManager(3);

    for (String c : collectors) {
      am.addCollector(c);
    }

    String agent1 =
        am.getFailChainSinkSpec("agent1", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent1);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 5\",\"collector 1\",\"collector 3\")",
        agent1);
    CompositeSink snk1 = new CompositeSink(new Context(), agent1);
    snk1.open();
    snk1.close();

    String agent2 =
        am.getFailChainSinkSpec("agent2", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent2);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 2\",\"collector 1\",\"collector 3\")",
        agent2);
    CompositeSink snk2 = new CompositeSink(new Context(), agent2);
    snk2.open();
    snk2.close();

    String agent3 =
        am.getFailChainSinkSpec("agent3", "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent3);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 5\",\"collector 1\",\"collector 2\")",
        agent3);
    CompositeSink snk3 = new CompositeSink(new Context(), agent3);
    snk3.open();
    snk3.close();

  }

}
