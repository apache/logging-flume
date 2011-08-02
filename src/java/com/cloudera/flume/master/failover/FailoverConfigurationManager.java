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

import static com.cloudera.flume.conf.PatternMatch.recursive;
import static com.cloudera.flume.conf.PatternMatch.var;

import java.util.List;
import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumePatterns;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.conf.PatternMatch;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.TranslatingConfigurationManager;
import com.cloudera.flume.master.Translator;
import com.cloudera.flume.master.availability.FailoverChainManager;
import com.google.common.base.Preconditions;

/**
 * This translates autoXXXsinks into full configurations.
 */
public class FailoverConfigurationManager extends
    TranslatingConfigurationManager implements Translator {
  final public static Logger LOG = Logger
      .getLogger(FailoverConfigurationManager.class);
  FailoverChainManager failchainMan;
  final public static String NAME = "FailoverTranslator";

  final public static String AUTO_BE = "autoBEChain";
  final public static String AUTO_DFO = "autoDFOChain";
  final public static String AUTO_E2E = "autoE2EChain";

  /**
   * Create Failover chain translating manager.
   */
  public FailoverConfigurationManager(ConfigurationManager parent,
      ConfigurationManager self, FailoverChainManager fcMan) {
    super(parent, self);
    Preconditions.checkArgument(fcMan != null);
    this.failchainMan = fcMan;
  }

  /**
   * Remove the logical node.
   */
  @Override
  public void removeLogicalNode(String logicNode) {
    failchainMan.removeCollector(logicNode);
    super.removeLogicalNode(logicNode);
  }

  /**
   * Sources that are collectors are not translated, however they are registered
   */
  @Override
  public String translateSource(String lnode, String source)
      throws FlumeSpecException {
    Preconditions.checkArgument(lnode != null);
    Preconditions.checkArgument(source != null);

    // register the source.
    if ("autoCollectorSource".equals(source)) {
      failchainMan.addCollector(lnode);
      source = "logicalSource"; // convert to logical source.
    } else {
      // remove if was previously a collector
      failchainMan.removeCollector(lnode);
    }
    return source;
  }

  /**
   * This translates all autoBEChain, autoE2EChain, and autoDFOChain into low
   * level sinks taking the failover chain mangers info into account.
   */
  @Override
  public String translateSink(String lnode, String sink)
      throws FlumeSpecException {
    Preconditions.checkArgument(lnode != null);
    Preconditions.checkArgument(sink != null);

    String xsink;
    try {
      List<String> failovers = failchainMan.getFailovers(lnode);
      xsink = FlumeSpecGen.genEventSink(substBEChains(sink, failovers));
      xsink = FlumeSpecGen.genEventSink(substDFOChains(xsink, failovers));
      xsink = FlumeSpecGen.genEventSink(substE2EChains(xsink, failovers));
      return xsink;
    } catch (RecognitionException e) {
      throw new FlumeSpecException(e.getMessage());
    }
  }

  /**
   * Takes a full sink specification and substitutes 'autoBEChain' with an
   * expanded best effort failover chain.
   */
  static CommonTree substBEChains(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {

    PatternMatch bePat = recursive(var("be", FlumePatterns.sink(AUTO_BE)));
    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> beMatches = bePat.match(sinkTree);

    if (beMatches == null) {
      // bail out early
      return sinkTree;
    }

    while (beMatches != null) {
      // found a autoBEChain, replace it with the chain.
      CommonTree beTree = beMatches.get("be");

      // generate
      CommonTree beFailChain = buildFailChainAST(
          "{ lazyOpen => { stubbornAppend => logicalSink(\"%s\") } }  ",
          collectors);

      // Check if beFailChain is null
      if (beFailChain == null) {
        beFailChain = FlumeBuilder
            .parseSink("fail(\"no physical collectors\")");
      }

      // subst
      int idx = beTree.getChildIndex();
      CommonTree parent = beTree.parent;
      if (parent == null) {
        sinkTree = beFailChain;
      } else {
        parent.replaceChildren(idx, idx, beFailChain);
      }
      // patern match again.
      beMatches = bePat.match(sinkTree);
    }
    return sinkTree;
  }

  /**
   * Takes a full sink specification and substitutes 'autoDFOChain' with an
   * expanded disk failover mode failover chain.
   */
  static CommonTree substDFOChains(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {
    PatternMatch dfoPat = recursive(var("dfo", FlumePatterns.sink(AUTO_DFO)));

    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> dfoMatches = dfoPat.match(sinkTree);
    if (dfoMatches == null) {
      return sinkTree;
    }

    while (dfoMatches != null) {
      // found a autoBEChain, replace it with the chain.
      CommonTree dfoTree = dfoMatches.get("dfo");
      CommonTree dfoFailChain = buildFailChainAST(
          "{ lazyOpen => { stubbornAppend => logicalSink(\"%s\") } }  ",
          collectors);

      // Check if dfo is null
      if (dfoFailChain == null) {
        dfoFailChain = FlumeBuilder.parseSink("fail(\"no collectors\")");
      }

      String dfo = "let primary := " + FlumeSpecGen.genEventSink(dfoFailChain)
          + " in "
          + "< primary ? {diskFailover => { insistentOpen =>  primary} } >";
      CommonTree newDfoTree = FlumeBuilder.parseSink(dfo);

      // subst
      int idx = dfoTree.getChildIndex();
      CommonTree parent = dfoTree.parent;
      if (parent == null) {
        sinkTree = newDfoTree;
      } else {
        parent.replaceChildren(idx, idx, newDfoTree);
      }
      // pattern match again.
      dfoMatches = dfoPat.match(sinkTree);
    }
    return sinkTree;
  }

  /**
   * Takes a full sink specification and substitutes 'autoE2EChain' with an
   * expanded wal+end2end ack chain.
   */
  static CommonTree substE2EChains(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {

    PatternMatch e2ePat = recursive(var("e2e", FlumePatterns.sink(AUTO_E2E)));
    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> e2eMatches = e2ePat.match(sinkTree);

    if (e2eMatches == null) {
      // bail out early.
      return sinkTree;
    }

    while (e2eMatches != null) {
      // found a autoBEChain, replace it with the chain.
      CommonTree beTree = e2eMatches.get("e2e");

      // generate
      CommonTree beFailChain = buildFailChainAST(
          "{ lazyOpen => { stubbornAppend => logicalSink(\"%s\") } }  ",
          collectors);

      // Check if beFailChain is null
      if (beFailChain == null) {
        beFailChain = FlumeBuilder.parseSink("fail(\"no collectors\")");
      }

      // subst
      int idx = beTree.getChildIndex();
      CommonTree parent = beTree.parent;
      if (parent == null) {
        sinkTree = beFailChain;
      } else {
        parent.replaceChildren(idx, idx, beFailChain);
      }

      // pattern match again.
      e2eMatches = e2ePat.match(sinkTree);

    }

    // wrap the sink with the ackedWriteAhead
    CommonTree wrapper = FlumeBuilder.parseSink("{ ackedWriteAhead => null}");
    PatternMatch nullPath = recursive(var("x", FlumePatterns.sink("null")));
    CommonTree replace = nullPath.match(wrapper).get("x");
    int idx = replace.getChildIndex();
    replace.parent.replaceChildren(idx, idx, sinkTree);
    return wrapper;
  }

  /**
   * This current version requires a "%s" that gets replaced with the value from
   * the list.
   * 
   * Warning! this is a potential security problem.
   */
  static CommonTree buildFailChainAST(String spec, List<String> collectors)
      throws FlumeSpecException, RecognitionException {

    // iterate through the list backwards
    CommonTree cur = null;
    for (int i = collectors.size() - 1; i >= 0; i--) {
      String s = collectors.get(i);
      // this should be a composite sink.
      String failoverSpec = String.format(spec, s);
      LOG.debug("failover spec is : " + failoverSpec);
      CommonTree branch = FlumeBuilder.parseSink(failoverSpec);
      if (cur == null) {
        cur = branch;
        continue;
      }
      String fail = "< " + FlumeSpecGen.genEventSink(branch) + " ? "
          + FlumeSpecGen.genEventSink(cur) + " >";
      cur = FlumeBuilder.parseSink(fail);
    }
    return cur;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return NAME;
  }

}
