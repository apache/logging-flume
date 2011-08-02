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

package com.cloudera.flume.conf;

import static com.cloudera.flume.conf.PatternMatch.kind;
import static com.cloudera.flume.conf.PatternMatch.or;
import static com.cloudera.flume.conf.PatternMatch.recursive;
import static com.cloudera.flume.conf.PatternMatch.tuple;
import static com.cloudera.flume.conf.PatternMatch.var;
import static com.cloudera.flume.conf.PatternMatch.wild;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * This tests the pattern matcher on generic CommonTrees
 */
public class TestPatternMatch {
  public static Logger LOG = Logger.getLogger(TestPatternMatch.class);

  // (DECO (SINK batch (DEC 10)) (SINK logicalNode (STRING "collector")))
  public static final String simple = " { batch(10) => logicalNode(\"collector\")  } ";

  @Before
  public void setDebug() {
    Logger.getLogger(TestPatternMatch.class).setLevel(Level.DEBUG);
  }

  public void dumpMatches(Map<String, CommonTree> matches) {
    if (matches == null) {
      LOG.info("no matches");
      return;
    }

    if (matches.size() == 0) {
      LOG.info("matches with no bindings");
      return;
    }

    for (Entry<String, CommonTree> pair : matches.entrySet()) {
      LOG.info(pair.getKey() + " = " + pair.getValue().toStringTree());
    }
  }

  @Test
  public void testVar() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = var("top", wild());
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    assertNotNull(m.get("top"));
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testKind() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = kind("DECO");
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testKindVar() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = var("top", kind("DECO"));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testKindFail() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = var("top", kind("NOTDECO"));
    Map<String, CommonTree> m = pp.match(sink);
    assertNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testChild() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = kind("DECO").child(
        kind("SINK").child(var("node", kind("batch"))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testAnyChild() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = kind("DECO").child(
        kind("SINK").child(var("node", kind("logicalNode"))));

    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testNth() throws RecognitionException {
    CommonTree sink = FlumeBuilder.parseSink(simple);
    PatternMatch pp = kind("DECO").nth(1,
        kind("SINK").child(var("node", kind("logicalNode"))));

    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    assertEquals(m.get("node").getText(), "logicalNode");
    dumpMatches(m);
    LOG.info(m);

    // This will attempt to match logicalNode to batch, which will fail
    pp = kind("DECO").nth(0,
        kind("SINK").child(var("node", kind("logicalNode"))));
    m = pp.match(sink);
    assertNull(m);
    dumpMatches(m);
    LOG.info(m);

  }

  @Test
  public void testRecursive() throws RecognitionException {
    CommonTree sink = FlumeBuilder
        .parseSink("let foo := < null ? [ console, { test => { test2 =>  { batch(10) => logicalNode(\"collector\")  } } } ] > in foo");
    LOG.info(sink.toStringTree());

    PatternMatch pp = recursive(var("batchsize", kind("DEC")));
    // 
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);

    pp = recursive(kind("SINK").child(var("LogicalNodeName", kind("STRING"))));
    m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);

  }

  @Test
  public void testTuple() throws RecognitionException {
    CommonTree sink = FlumeBuilder
        .parseSink(" { batch(10) => logicalNode(\"collector\")  } ");
    PatternMatch pp = recursive(tuple(kind("SINK"), kind("logicalNode"), var(
        "LogicalNodeName", kind("STRING"))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testOr() throws RecognitionException {
    CommonTree sink = FlumeBuilder
        .parseSink(" { batch(10) => logicalNode(\"collector\")  } ");
    // PatternMatch pdeco = var("deco", kind("DECO"));
    PatternMatch pdeco = tuple(kind("DECO"), var("deco", kind("SINK")), var(
        "child", kind("SINK")));

    /*
     * .child( tuple(var("deco", kind("SINK")), var("child", wild())));
     */
    PatternMatch psink = tuple(kind("SINK"), var("name", wild()), var("sink",
        kind("STRING")));
    PatternMatch pp = or(psink, pdeco);
    LOG.info(sink.toStringTree());

    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    assertTrue(m.containsKey("deco"));
    dumpMatches(m);
    LOG.info(m);

    m = pp.match(m.get("child"));
    assertNotNull(m);
    assertTrue(m.containsKey("sink"));
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testSubst() throws RecognitionException, FlumeSpecException {
    CommonTree sink = FlumeBuilder
        .parseSink(" { batch(10) => logicalNode(\"collector\")  } ");

    PatternMatch pp = recursive(var("logical", tuple(kind("SINK"),
        kind("logicalNode"), var("LogicalNodeName", kind("STRING")))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    dumpMatches(m);

    LOG.info(sink.toStringTree());

    CommonTree fix = m.get("logical");
    LOG.info(fix.toStringTree());

    String lname = FlumeBuilder.buildArg(m.get("LogicalNodeName"));
    LOG.info("LogicalName " + lname);

    CommonTree replace = FlumeBuilder.parseSink("rpcSink(\"foo\",12345)");
    LOG.info(replace.toStringTree());

    fix.parent.replaceChildren(1, 1, replace);

    LOG.info(sink.toStringTree());
    String out = FlumeSpecGen.genEventSink(sink);
    LOG.info(out);
    assertEquals("{ batch( 10 ) => rpcSink( \"foo\", 12345 ) }", out);
  }

  @Test
  public void testShadow() throws RecognitionException, FlumeSpecException {
    String shadowed = "[ logicalNode(\"collector1\"), logicalNode(\"collector2\"), logicalNode(\"collector3\") ]";
    CommonTree sink = FlumeBuilder.parseSink(shadowed);
    LOG.info(sink.toStringTree());

    PatternMatch pp = recursive(kind("SINK").child(
        var("ln", kind("logicalNode"))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    CommonTree lnCt = (CommonTree) m.get("ln").getParent();
    assertEquals("collector1", FlumeBuilder.buildArg((CommonTree) lnCt
        .getChild(1)));
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testVarTupleShadow() throws RecognitionException,
      FlumeSpecException {
    String shadowed = "[ logicalNode(\"collector1\"), logicalNode(\"collector2\"), logicalNode(\"collector3\") ]";
    CommonTree sink = FlumeBuilder.parseSink(shadowed);
    LOG.info(sink.toStringTree());

    PatternMatch pp = recursive(var("ln", kind("SINK").child(
        var("ln", kind("logicalNode")))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    CommonTree lnCt = m.get("ln");
    assertEquals("SINK", lnCt.getText());
    dumpMatches(m);
    LOG.info(m);

    // shadow -- left wins. (could be collector1, collector2, or collector 3)
    pp = recursive(tuple(kind("MULTI"), var("ln", kind("SINK")), var("ln",
        kind("SINK")), var("ln", kind("SINK"))));
    m = pp.match(sink);
    assertNotNull(m);
    lnCt = (CommonTree) m.get("ln").getChild(1);
    assertEquals("collector1", FlumeBuilder.buildArg(lnCt));
    dumpMatches(m);
    LOG.info(m);

    // shadow -- parent wins. (could be MULTI, or collector1,2,3)
    pp = recursive(tuple(var("ln", kind("MULTI")), var("ln", kind("SINK")),
        var("ln", kind("SINK")), var("ln", kind("SINK"))));
    m = pp.match(sink);
    assertNotNull(m);
    lnCt = (CommonTree) m.get("ln");
    assertEquals("MULTI", lnCt.getText());
    dumpMatches(m);
    LOG.info(m);
  }

  @Test
  public void testVarParentShadow() throws RecognitionException,
      FlumeSpecException {
    String shadowed = "{ null0 => {  null1 => [ logicalNode(\"collector1\"), logicalNode(\"collector2\"), logicalNode(\"collector3\") ] } }";
    CommonTree sink = FlumeBuilder.parseSink(shadowed);
    LOG.info(sink.toStringTree());

    // parent wins (pattern could match null0 or null1)
    PatternMatch pp = recursive(var("ln", kind("DECO").child(
        recursive(var("ln", kind("logicalNode"))))));
    Map<String, CommonTree> m = pp.match(sink);
    assertNotNull(m);
    CommonTree lnCt = (CommonTree) m.get("ln").getChild(0).getChild(0);
    assertEquals("null0", lnCt.getText());
    dumpMatches(m);
    LOG.info(m);
  }
}
