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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final Logger LOG = LoggerFactory
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
  public void removeLogicalNode(String logicNode) throws IOException {
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
      xsink = FlumeSpecGen.genEventSink(substDFOChainsNoLet(xsink, failovers));
      xsink = FlumeSpecGen.genEventSink(substE2EChainsSimple(xsink, failovers));
      return xsink;
    } catch (RecognitionException e) {
      throw new FlumeSpecException(e.getMessage());
    }
  }

  /**
   * Takes a full sink specification and substitutes 'autoBEChain' with an
   * expanded best effort failover chain.
   * 
   * 'autoBEChain' gets translated to
   * 
   * < < logicalSink(arg1) ? <... ? null > > >
   * 
   * Basically this will try to to send data to logicalsink(arg1), and failing
   * that to logicalsink(arg2) etc. If all logicalSinks fail it will fall back
   * to a null sink which drops messages. The failover sink by default has a
   * timed backoff policy and will reattempt opening and sending to the
   * different sink in the failover chains.
   * 
   */
  static CommonTree substBEChains(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {

    PatternMatch bePat = recursive(var("be", FlumePatterns.sink(AUTO_BE)));
    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> beMatches = bePat.match(sinkTree);

    ArrayList<String> collSnks = new ArrayList<String>();
    for (String coll : collectors) {
      collSnks.add("{ lazyOpen => logicalSink(\"" + coll + "\") }");
    }
    collSnks.add("null");

    if (beMatches == null) {
      // bail out early
      return sinkTree;
    }

    while (beMatches != null) {
      // found a autoBEChain, replace it with the chain.
      CommonTree beTree = beMatches.get("be");

      // generate
      CommonTree beFailChain = buildFailChainAST("%s", collSnks);

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
   * This is a simpler DFO that doesn't use let statements. This approach will
   * use more resources (more ports used up on this node and the downstream node
   * because of no sharing). Unfortunately 'let's end up being very tricky to
   * use in the cases where failures occur, and need more thought.
   * 
   * 'autoDFOChain' becomes:
   * 
   * < < logicalSink(arg1) ? ... > ? { diskFailover => { insistentAppend => {
   * stubbornAppend => { insistentOpen => < logicalSink(arg1) ? ... > } } } } >
   * 
   * This pipeline writes attempts to send data to each of the logical node
   * arg1, then then to logical node arg2, etc. If the logical nodes all fail,
   * we go to the diskFailover, which writes data to the local log. The subsink
   * of the diskFailover has a subservient DriverThread that will attempt to
   * send data to logical sink arg1, and then to logical sink arg2, etc.. If all
   * fail of these fail, the stubbornAppend causes the entire failover chain to
   * be closed and then reopened. The insistentOpen insistentOpen ensures that
   * they are tried again after an backing off. Stubborn append gives up after a
   * second failure -- the insistentAppend wrapping it ensures that it will
   * continue retrying while the sink is open.
   */
  static CommonTree substDFOChainsNoLet(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {
    PatternMatch dfoPat = recursive(var("dfo", FlumePatterns.sink(AUTO_DFO)));

    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> dfoMatches = dfoPat.match(sinkTree);
    if (dfoMatches == null) {
      return sinkTree;
    }

    while (dfoMatches != null) {
      // found a autoDFOChain, replace it with the chain.
      CommonTree dfoTree = dfoMatches.get("dfo");

      // All the logical sinks are lazy individually
      CommonTree dfoPrimaryChain = buildFailChainAST(
          "{ lazyOpen => logicalSink(\"%s\") }", collectors);
      // Check if dfo is null
      if (dfoPrimaryChain == null) {
        dfoPrimaryChain = FlumeBuilder.parseSink("fail(\"no collectors\")");
      }

      // diskfailover's subsink needs to never give up. So we wrap it with an
      // insistentAppend. But append can fail if its subsink is not open. So
      // we add a stubborn append (it closes and reopens a subsink) and retries
      // opening the chain using the insistentOpen
      String dfo = "< " + FlumeSpecGen.genEventSink(dfoPrimaryChain)
          + "  ? {diskFailover => "
          + "{ insistentAppend => { stubbornAppend => { insistentOpen =>"
          + FlumeSpecGen.genEventSink(dfoPrimaryChain) + " } } } } >";
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
   * Takes a full sink specification and substitutes 'autoDFOChain' with an
   * expanded disk failover mode failover chain.
   * 
   * This version is deprecated because it uses 'let' expressions. 'let'
   * expressions semantics are not clear in the face of failures.
   */
  @Deprecated
  static CommonTree substDFOChains(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {
    PatternMatch dfoPat = recursive(var("dfo", FlumePatterns.sink(AUTO_DFO)));

    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> dfoMatches = dfoPat.match(sinkTree);
    if (dfoMatches == null) {
      return sinkTree;
    }

    while (dfoMatches != null) {
      // found a autoDFOChain, replace it with the chain.
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
   * 
   * This version at one point was different from substE2EChainSimple's
   * implementation but they have not converged. This one should likely be
   * removed in the future.
   */
  @Deprecated
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
      // found a autoE2EChain, replace it with the chain.
      CommonTree beTree = e2eMatches.get("e2e");

      // generate
      CommonTree beFailChain = buildFailChainAST("logicalSink(\"%s\") ",
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
    CommonTree wrapper = FlumeBuilder
        .parseSink("{ ackedWriteAhead => { stubbornAppend => { insistentOpen => null } } }");
    PatternMatch nullPath = recursive(var("x", FlumePatterns.sink("null")));
    CommonTree replace = nullPath.match(wrapper).get("x");
    int idx = replace.getChildIndex();
    replace.parent.replaceChildren(idx, idx, sinkTree);
    return wrapper;
  }

  /**
   * Takes a full sink specification and substitutes 'autoE2EChain' with an
   * expanded wal+end2end ack chain. It just replaces the sink and does not
   * attempt any sandwiching of decorators
   * 
   * 'autoE2EChain' becomes:
   * 
   * { ackedWriteAhead => { stubbornAppend => { insistentOpen => <
   * logicalSink(arg1) ? ... > } } }
   * 
   * This pipeline writes data to the WAL adding ack tags. In the WAL's subsink
   * in a subservient DriverThread will attempt to send data to logical sink
   * arg1, and then to logical sink arg2, etc.. If all fail, stubbornAppend
   * causes the entire failover chain to be closed and then reopened. If all the
   * elements of the failover chain still fail, the insistentOpen ensures that
   * they are tried again after an backing off.
   */
  static CommonTree substE2EChainsSimple(String sink, List<String> collectors)
      throws RecognitionException, FlumeSpecException {

    PatternMatch e2ePat = recursive(var("e2e", FlumePatterns.sink(AUTO_E2E)));
    CommonTree sinkTree = FlumeBuilder.parseSink(sink);
    Map<String, CommonTree> e2eMatches = e2ePat.match(sinkTree);

    if (e2eMatches == null) {
      // bail out early.
      return sinkTree;
    }

    while (e2eMatches != null) {
      // found a autoE2EChain, replace it with the chain.
      CommonTree e2eTree = e2eMatches.get("e2e");

      // generate
      CommonTree e2eFailChain = buildFailChainAST("logicalSink(\"%s\") ",
          collectors);

      // Check if beFailChain is null
      if (e2eFailChain == null) {
        e2eFailChain = FlumeBuilder.parseSink("fail(\"no collectors\")");
      }

      // now lets wrap the beFailChain with the ackedWriteAhead
      String translated = "{ ackedWriteAhead => { stubbornAppend => { insistentOpen => "
          + FlumeSpecGen.genEventSink(e2eFailChain) + " } } }";
      CommonTree wrapper = FlumeBuilder.parseSink(translated);

      // subst
      int idx = e2eTree.getChildIndex();
      CommonTree parent = e2eTree.parent;
      if (parent == null) {
        sinkTree = wrapper;
      } else {
        parent.replaceChildren(idx, idx, wrapper);
      }

      // pattern match again.
      e2eMatches = e2ePat.match(sinkTree);
    }

    // wrap the sink with the ackedWriteAhead
    return sinkTree;
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
