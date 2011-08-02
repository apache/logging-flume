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

package com.cloudera.flume.master.logical;

import static com.cloudera.flume.conf.PatternMatch.kind;
import static com.cloudera.flume.conf.PatternMatch.recursive;
import static com.cloudera.flume.conf.PatternMatch.var;

import java.io.IOException;
import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumePatterns;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.conf.PatternMatch;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.TranslatingConfigurationManager;
import com.cloudera.flume.master.Translator;
import com.cloudera.flume.master.logical.LogicalNameManager.PhysicalNodeInfo;

/**
 * This configuration manager translates users specified logical configurations
 * into physical configurations.
 */
public class LogicalConfigurationManager extends
    TranslatingConfigurationManager implements Translator {
  static final Logger LOG = LoggerFactory.getLogger(LogicalConfigurationManager.class);
  public static final String NAME = "LogicalTranslator";
  final LogicalNameManager nameMan;

  /**
   * Construct a LogicalConfigurationManager with specified parent, self and
   * StatusManagers.
   */
  public LogicalConfigurationManager(ConfigurationManager parent,
      ConfigurationManager self, StatusManager statman) {
    super(parent, self);
    this.nameMan = new LogicalNameManager(parent, statman);
  }

  /**
   * This method takes a logical sink and returns an AST of its physical sink
   * representation.
   **/
  CommonTree substLogicalSink(String sink) throws RecognitionException,
      FlumeSpecException {
    CommonTree lsnkTree = FlumeBuilder.parseSink(sink);
    LOG.debug(lsnkTree.toStringTree());
    PatternMatch p = recursive(var("lsnk", kind("SINK").child(
        kind("logicalSink"))));
    Map<String, CommonTree> matches = p.match(lsnkTree);

    if (matches == null) {
      // do nothing,
      return lsnkTree;
    }

    CommonTree lsnk = matches.get("lsnk");
    final String orig = StringEscapeUtils.escapeJava(FlumeSpecGen
        .genEventSink(lsnk));

    String tgtLn = FlumeBuilder.buildArg((CommonTree) lsnk.getChild(1));

    PhysicalNodeInfo pni = nameMan.getPhysicalNodeInfo(tgtLn);

    String tgtPhysNode = getPhysicalNode(tgtLn);

    if (tgtPhysNode == null || pni == null) {
      // force a translation to an valid but incomplete translation
      pni = new PhysicalNodeInfo() {
        @Override
        public String getPhysicalSink() {
          return "fail( \"" + orig + "\" )";
        }

        @Override
        public String getPhysicalSource() {
          return "fail";
        }
      };
    }

    String snk = pni.getPhysicalSink();
    if (snk == null) {
      // bail out
      return null;
    }

    CommonTree psnkTree = FlumeBuilder.parseSink(snk);
    PatternMatch.replaceChildren(lsnk, psnkTree);
    return lsnkTree;
  }

  /**
   * This method gets a logical node 'ln' and a logicalSource 'source', and
   * substitutes in a physical source.
   */
  CommonTree substLogicalSource(String ln, String source)
      throws RecognitionException {
    CommonTree lsrcTree = FlumeBuilder.parseSource(source);
    LOG.debug(lsrcTree.toStringTree());
    PatternMatch p = FlumePatterns.source("logicalSource");
    Map<String, CommonTree> matches = p.match(lsrcTree);

    if (matches == null) {
      // if was previously a logical source, unregister it.
      nameMan.setPhysicalNode(ln, null);

      // do nothing,
      return lsrcTree;
    }

    // check logical name manager
    PhysicalNodeInfo pni = nameMan.getPhysicalNodeInfo(ln);
    if (pni == null) {
      // bail out
      nameMan.updateNode(ln);
      pni = nameMan.getPhysicalNodeInfo(ln);
      if (pni == null) {
        // return failure.
        return null;
      }
    }

    String src = pni.getPhysicalSource();

    String phys = getPhysicalNode(ln);
    if (phys == null) {
      src = "fail( \"no physical translation for " + ln + "\" )";
    }

    if (src == null) {
      LOG.warn("Physical Source for " + ln + " not translated");
      return null;
    }
    CommonTree psrcTree = FlumeBuilder.parseSource(src);
    PatternMatch.replaceChildren(lsrcTree, psrcTree);

    return lsrcTree;
  }

  /**
   * Translates a logical source into a physical source
   */
  public String translateSource(String logicalnode, String source)
      throws FlumeSpecException {
    try {
      CommonTree pCt = substLogicalSource(logicalnode, source);
      if (pCt == null) {
        return source;
      }
      String pStr = FlumeSpecGen.genEventSource(pCt);
      return pStr;
    } catch (RecognitionException e) {
      LOG.error("Problem with physical sink", e);
    }
    return null;
  }

  /**
   * Translates a logical sink into a physical sink
   */
  public String translateSink(String logicalnode, String sink)
      throws FlumeSpecException {
    try {
      String cur = sink;
      String last = null;
      while (!cur.equals(last)) {

        CommonTree pCt = substLogicalSink(cur);
        if (pCt == null) {
          return cur;
        }
        last = cur;
        cur = FlumeSpecGen.genEventSink(pCt);
      }
      return cur;
    } catch (RecognitionException e) {
      LOG.error("Problem with physical sink", e);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void refreshAll() throws IOException {
    try {
      nameMan.update();
    } catch (RecognitionException e) {
      LOG.error("Internal Error: " + e.getLocalizedMessage(), e);
      throw new IOException("Internal Error: " + e.getMessage());
    }
    super.refreshAll();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateAll() throws IOException {
    try {
      nameMan.update();
    } catch (RecognitionException e) {
      LOG.error("Internal Error: " + e.getLocalizedMessage(), e);
      throw new IOException("Internal Error: " + e.getMessage());
    }
    super.updateAll();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return NAME;
  }

}
