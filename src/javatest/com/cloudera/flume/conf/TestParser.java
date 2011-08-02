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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.ExampleData;

/**
 * This tests a swath of the language features for the flume configuration
 * language. All should throw exceptions on parser or lexer failures.
 */
public class TestParser implements ExampleData {

  final static Logger LOG = Logger.getLogger(TestParser.class.getName());

  /**
   * Test parsing of literals.
   */
  @Test
  public void testLiteralParser() throws RecognitionException {
    LOG.info("== literals ==");

    String s = "1234";
    Object o = FlumeBuilder.parseLiteral(s);
    LOG.info(toTree(o));
    assertEquals("(DEC 1234)", toTree(o));

    String s1 = "1234.234";
    Object o1 = FlumeBuilder.parseLiteral(s1);
    LOG.info(toTree(o1));
    assertEquals("(FLOAT 1234.234)", toTree(o1));

    String s2 = "\"string\"";
    Object o2 = FlumeBuilder.parseLiteral(s2);
    LOG.info(toTree(o2));
    assertEquals("(STRING \"string\")", toTree(o2));

    String s3 = "true";
    Object o3 = FlumeBuilder.parseLiteral(s3);
    LOG.info(toTree(o3));
    assertEquals("(BOOL true)", toTree(o3));

  }

  /**
   * Strings are allowed to have some java escape sequences, make sure they are
   * unescaped when values are instantiated.
   */
  @Test
  public void testJavaStringEscape() throws RecognitionException,
      FlumeSpecException {
    String s2x = "\"\\\"string\\\"\"";
    CommonTree o2x = FlumeBuilder.parseLiteral(s2x);
    LOG.info(toTree(o2x));
    // assertEquals("\"string\"", FlumeBuilder.buildArg(o2x));
    assertEquals("\"string\"", FlumeBuilder.buildArg(o2x));
  }

  String toTree(Object o) {
    return ((CommonTree) o).toStringTree();
  }

  @Test
  public void testSinkParser() throws RecognitionException {
    LOG.info("== sinks ==");

    String s = "text";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(s);
    LOG.info(toTree(o));
    assertEquals("(SINK text)", toTree(o));

    String s2 = "text(\"bogusdata\")";

    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(s2);
    LOG.info(toTree(o2));
    assertEquals("(SINK text (STRING \"bogusdata\"))", toTree(o2));

    String s3 = "[text(true)]";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(s3);
    LOG.info(toTree(o3));
    assertEquals("(MULTI (SINK text (BOOL true)))", toTree(o3));

    String s4 = "[text(true) , tail(\"somthingelse\") ]";
    Object o4 = FlumeBuilder.parseSink(s4);
    LOG.info(s4);
    LOG.info(toTree(o4));
    assertEquals(
        "(MULTI (SINK text (BOOL true)) (SINK tail (STRING \"somthingelse\")))",
        toTree(o4));
  }

  @Test
  public void testBuilder() throws RecognitionException {
    LOG.info("== nodes ==");

    // String s = "source : tail(\"/var/log/httpd/access_log\") | thrift;";
    String s = "nodename : nodesource | nodesink;";
    Object o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));
    assertEquals("(NODE nodename (SOURCE nodesource) (SINK nodesink)) null",
        toTree(o));
  }

  @Test
  public void testDecorator() throws RecognitionException {
    LOG.info("== Decorators ==");

    String s = "{ deco => nodesink } ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(DECO (SINK deco) (SINK nodesink))", toTree(o));

    String s3 = "{ deco1 => [ sink1, sink2] } ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals("(DECO (SINK deco1) (MULTI (SINK sink1) (SINK sink2)))",
        toTree(o3));

    // Test a "tight decorator" -- no extra spaces
    String s4 = "{deco1=>sink1} ";
    Object o4 = FlumeBuilder.parseSink(s4);
    LOG.info(toTree(o4));
    assertEquals("(DECO (SINK deco1) (SINK sink1))", toTree(o4));
  }

  @Test(expected = RuntimeRecognitionException.class)
  public void testBadDecorator() throws RecognitionException {
    // deco on left side of deco is not legal.
    String s2 = "{{ deco1 => deco22 } => nodesink } ";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals("(DECO (DECO (SINK deco1) (SINK deco22)) (SINK nodesink))",
        toTree(o2));
  }

  public void testFailover() throws RecognitionException {
    LOG.info("== Failover ==");

    String s = "< deco ? nodesink > ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(BACKUP (SINK deco) (SINK nodesink))", toTree(o));

    String s2 = "<<  deco1 ? deco22 > ? nodesink > ";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(BACKUP (BACKUP (SINK deco1) (SINK deco22)) (SINK nodesink))",
        toTree(o2));

    String s3 = "< deco1 ? [ sink1, sink2] > ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals("(BACKUP (SINK deco1) (MULTI (SINK sink1) (SINK sink2)))",
        toTree(o3));
  }

  /**
   * These parse to make sure it works as a root, complex sub sinks work and
   * that it works as a subsink, and can compose other lets (in the arg and body
   * slots).
   */
  public void testLet() throws RecognitionException {
    LOG.info("== Let ==");

    String s = "let foo := console in foo ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(LET foo (SINK console) (SINK foo))", toTree(o));

    String s2 = "let foo := console in < foo ? [ console, foo] >";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(LET foo (SINK console) (BACKUP (SINK foo) (MULTI (SINK console) (SINK foo))))",
        toTree(o2));

    String s3 = "[ let foo := console in foo, let bar := console in bar ]";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals(
        "(MULTI (LET foo (SINK console) (SINK foo)) (LET bar (SINK console) (SINK bar)))",
        toTree(o3));

    String s4 = "let foo := console in let bar := console in [ foo, bar ]";
    Object o4 = FlumeBuilder.parseSink(s4);
    LOG.info(toTree(o4));
    assertEquals(
        "(LET foo (SINK console) (LET bar (SINK console) (MULTI (SINK foo) (SINK bar))))",
        toTree(o4));

    String s5 = "let foo := let bar := console in bar in foo";
    Object o5 = FlumeBuilder.parseSink(s5);
    LOG.info(toTree(o5));
    assertEquals("(LET foo (LET bar (SINK console) (SINK bar)) (SINK foo))",
        toTree(o5));

  }

  public void testCombo() throws RecognitionException {
    LOG.info("== Combo ==");

    String s = "< deco ? [nodesink, nodesink2] > ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals(
        "(BACKUP (SINK deco) (MULTI (SINK nodesink) (SINK nodesink2)))",
        toTree(o));

    String s2 = "{ deco1 =>  <<  deco1 ? deco22 > ? nodesink > }";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(DECO (SINK deco1) (BACKUP (BACKUP (SINK deco1) (SINK deco22)) (SINK nodesink)))",
        toTree(o2));

    String s3 = "< deco1 ? [ {sampler => sink1 } , sink2] > ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals(
        "(BACKUP (SINK deco1) (MULTI (DECO (SINK sampler) (SINK sink1)) (SINK sink2)))",
        toTree(o3));
  }

  @Test(expected = RuntimeRecognitionException.class)
  public void testLexFails() throws RecognitionException {
    String s = "12345.123423412.123.41.3.";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
  }

  @Test(expected = RuntimeRecognitionException.class)
  public void testParseFails() throws RecognitionException {
    String s = "< deoc asdf fial blah } >";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
  }

  @Test
  public void testNode() throws RecognitionException {
    LOG.info("== Node ==");

    String s = "host: source | < deco ? [nodesink, nodesink2] > ; ";
    Object o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));

    // names with numbers
    s = "host123: source | < deco ? [nodesink, nodesink2] > ; ";
    o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));

    // with dashes, underscore
    s = "host-with_dashes: source | < deco ? [nodesink, nodesink2] > ; ";
    o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));

    // dns name
    s = "name.host.com: source | < deco ? [nodesink, nodesink2] > ; ";
    o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));

    // ip address
    s = "1.2.3.4: source | < deco ? [nodesink, nodesink2] > ; ";
    o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));

    // wrong ip address
    try {
      s = "1.2.3.4.324: source | < deco ? [nodesink, nodesink2] > ; ";
      o = FlumeBuilder.parse(s);
      LOG.info(toTree(o));
      fail("This should throw exception");
    } catch (RuntimeException e) {
      // we are ok.
    }

    // wrong ip address
    try {
      s = "-blah: source | < deco ? [nodesink, nodesink2] > ; ";
      o = FlumeBuilder.parse(s);
      LOG.info(toTree(o));
      fail("This should throw exception");
    } catch (RuntimeException e) {
      // we are ok.
    }

    // wrong ip address
    try {
      s = "1234.-blah: source | < deco ? [nodesink, nodesink2] > ; ";
      o = FlumeBuilder.parse(s);
      LOG.info(toTree(o));
      fail("This should throw exception");
    } catch (RuntimeException e) {
      // we are ok.
    }

  }

}
