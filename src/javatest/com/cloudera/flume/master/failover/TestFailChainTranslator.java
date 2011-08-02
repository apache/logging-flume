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

package com.cloudera.flume.master.failover;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.master.availability.ConsistentHashFailoverChainManager;
import com.cloudera.flume.master.availability.FailoverChainManager;
import com.cloudera.flume.master.availability.TestFailChainManager;

/**
 * This verifies that updates happen properly when a fail chain manager's
 * collector node change. Most of these tests only check to make sure the
 * configuration length remains the same instead of writing out the fully
 * translated configurations.
 */
public class TestFailChainTranslator {

  static Logger LOG = Logger.getLogger(TestFailChainManager.class);
  static String[] collectors = { "collector 1", "collector 2", "collector 3",
      "collector 4", "collector 5" };

  @Before
  public void setDebug() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Test
  public void testReplaceFailchain() throws FlumeSpecException,
      RecognitionException {
    List<String> collectors = new ArrayList<String>();
    collectors.add("collector1");
    collectors.add("collector2");
    collectors.add("collector3");
    CommonTree failchain = FailoverConfigurationManager.buildFailChainAST(
        "{lazyOpen => counter(\"%s\") }", collectors);
    LOG.info(FlumeSpecGen.genEventSink(failchain));
  }

  @Test
  public void testSubstBE() throws FlumeSpecException, RecognitionException {
    List<String> collectors = new ArrayList<String>();
    collectors.add("collector1");
    collectors.add("collector2");
    collectors.add("collector3");

    // autoBEChain substituted
    CommonTree failchain = FailoverConfigurationManager.substBEChains(
        "autoBEChain", collectors);
    String failChainSink = FlumeSpecGen.genEventSink(failchain);
    LOG.info(failChainSink);
    assertEquals(154, failChainSink.length()); // output is 154 chars long

    // many autoBEChain substitutions.
    CommonTree failchain2 = FailoverConfigurationManager.substBEChains(
        "[ autoBEChain, { lazyOpen => autoBEChain } ]", collectors);
    String failChainSink2 = FlumeSpecGen.genEventSink(failchain2);
    LOG.info(failChainSink2);
    assertEquals(330, failChainSink2.length()); // output is 330 chars long

    // no change
    CommonTree nothing = FailoverConfigurationManager.substBEChains("null",
        collectors);
    String nothingSink = FlumeSpecGen.genEventSink(nothing);
    assertEquals("null", nothingSink);
  }

  @Test
  public void testSubstDFO() throws FlumeSpecException, RecognitionException {
    List<String> collectors = new ArrayList<String>();
    collectors.add("collector1");
    collectors.add("collector2");
    collectors.add("collector3");

    // autoDFOChain substituted
    CommonTree failchain = FailoverConfigurationManager.substDFOChainsNoLet(
        "autoDFOChain", collectors);
    String failChainSink = FlumeSpecGen.genEventSink(failchain);
    LOG.info(failChainSink);
    assertEquals(379, failChainSink.length()); // output is 379 chars long

    // many autoDFOChain substitutions.
    CommonTree failchain2 = FailoverConfigurationManager.substDFOChainsNoLet(
        "[ autoDFOChain, { lazyOpen => autoDFOChain } ]", collectors);
    String failChainSink2 = FlumeSpecGen.genEventSink(failchain2);
    LOG.info(failChainSink2);
    assertEquals(780, failChainSink2.length()); // output is 780 chars long

    // no change
    CommonTree nothing = FailoverConfigurationManager.substDFOChainsNoLet(
        "null", collectors);
    String nothingSink = FlumeSpecGen.genEventSink(nothing);
    assertEquals("null", nothingSink);
  }

  @Test
  public void testSubstE2E() throws FlumeSpecException, RecognitionException {
    List<String> collectors = new ArrayList<String>();
    collectors.add("collector1");
    collectors.add("collector2");
    collectors.add("collector3");

    // autoE2EChain substituted
    CommonTree failchain = FailoverConfigurationManager.substE2EChains(
        "autoE2EChain", collectors);
    String failChainSink = FlumeSpecGen.genEventSink(failchain);
    LOG.info(failChainSink);
    // output is 161 chars long (translation checked in
    // TestMasterAutoUpdatesE2E)
    assertEquals(161, failChainSink.length());

    // many autoE2EChain substitutions.
    CommonTree failchain2 = FailoverConfigurationManager.substE2EChains(
        "[ autoE2EChain, { lazyOpen => autoE2EChain } ]", collectors);
    String failChainSink2 = FlumeSpecGen.genEventSink(failchain2);
    LOG.info(failChainSink2);
    // output is 278 chars long (translation checked in
    // TestMasterAutoUpdatesE2E)
    assertEquals(278, failChainSink2.length());

    // no change
    CommonTree nothing = FailoverConfigurationManager.substE2EChains("null",
        collectors);
    String nothingSink = FlumeSpecGen.genEventSink(nothing);
    assertEquals("null", nothingSink);

  }

  @Test
  public void testSubstE2ESimple() throws FlumeSpecException,
      RecognitionException {
    List<String> collectors = new ArrayList<String>();
    collectors.add("collector1");
    collectors.add("collector2");
    collectors.add("collector3");

    // autoE2EChain substituted
    CommonTree failchain = FailoverConfigurationManager.substE2EChainsSimple(
        "autoE2EChain", collectors);
    String failChainSink = FlumeSpecGen.genEventSink(failchain);
    LOG.info(failChainSink);
    // output is 232 chars long (translation checked in
    // TestMasterAutoUpdatesE2E)
    assertEquals(161, failChainSink.length());

    // many autoE2EChain substitutions.
    CommonTree failchain2 = FailoverConfigurationManager.substE2EChainsSimple(
        "[ autoE2EChain, { lazyOpen => autoE2EChain } ]", collectors);
    String failChainSink2 = FlumeSpecGen.genEventSink(failchain2);
    LOG.info(failChainSink2);
    // output is 344 chars long (translation checked in
    // TestMasterAutoUpdatesE2E)
    assertEquals(344, failChainSink2.length());

    // no change
    CommonTree nothing = FailoverConfigurationManager.substE2EChainsSimple(
        "null", collectors);
    String nothingSink = FlumeSpecGen.genEventSink(nothing);
    assertEquals("null", nothingSink);

  }

  @Test
  public void testConsistentHashAvailMan() throws FlumeSpecException,
      IOException, InterruptedException {
    FailoverChainManager am = new ConsistentHashFailoverChainManager(3);

    for (String c : collectors) {
      am.addCollector(c);
    }

    String agent1 = am.getFailChainSinkSpec("agent1",
        "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent1);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 2\",\"collector 3\",\"collector 1\")",
        agent1);
    CompositeSink snk1 = new CompositeSink(new Context(), agent1);
    snk1.open();
    snk1.close();

    String agent2 = am.getFailChainSinkSpec("agent2",
        "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent2);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 1\",\"collector 4\",\"collector 5\")",
        agent2);
    CompositeSink snk2 = new CompositeSink(new Context(), agent2);
    snk2.open();
    snk2.close();

    String agent3 = am.getFailChainSinkSpec("agent3",
        "{lazyOpen => counter(\"%s\")}");
    LOG.info(agent3);
    assertEquals(
        "failChain(\"{lazyOpen => counter(\\\"%s\\\")}\",\"collector 1\",\"collector 2\",\"collector 5\")",
        agent3);
    CompositeSink snk3 = new CompositeSink(new Context(), agent3);
    snk3.open();
    snk3.close();

  }

}
