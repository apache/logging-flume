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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;

/**
 * This tests a swath of the language features for the flume configuration
 * language. All should throw exceptions on parser or lexer failures.
 */
public class TestParser implements ExampleData {

  static final Logger LOG = LoggerFactory.getLogger(TestParser.class);

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
    assertEquals("\"string\"", FlumeBuilder.buildSimpleArg(o2x));
  }

  String toTree(Object o) {
    return ((CommonTree) o).toStringTree();
  }

  @Test
  public void testKeywordArgParser() throws RecognitionException {
    LOG.info("== kw args ==");

    // kwargs only
    String s = "text(bogus=\"bogusdata\",foo=\"bar\")";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(s);
    LOG.info(toTree(o));
    assertEquals(
        "(DECO (SINK text (KWARG bogus (STRING \"bogusdata\")) (KWARG foo (STRING \"bar\"))))",
        toTree(o));

    // normal arg then kwargs
    String s2 = "text(\"bogusdata\",foo=\"bar\")";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(DECO (SINK text (STRING \"bogusdata\") (KWARG foo (STRING \"bar\"))))",
        toTree(o2));

    // normal arg then kwargs
    s2 = "text(\"bogusdata\",foo=\"bar\", boo=1.5)";
    o2 = FlumeBuilder.parseSink(s2);
    LOG.info(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(DECO (SINK text (STRING \"bogusdata\") (KWARG foo (STRING \"bar\")) (KWARG boo (FLOAT 1.5))))",
        toTree(o2));

  }

  /**
   * Reject kw arg before normal args. Our parser converts all exceptions into
   * RuntimeRecoginitionExceptions.
   */
  @Test(expected = RuntimeRecognitionException.class)
  public void testBadKWArgOrder() throws RecognitionException {
    // kwargs then normal arg not cool
    String s2 = "text(foo=\"bar\",\"bogusdata\")";
    FlumeBuilder.parseSink(s2);
  }

  @Test
  public void testKWArgContextSink() throws RecognitionException,
      FlumeSpecException {
    class ContextSink extends EventSink.Base {
      public Context ctx;
      public String[] argv;

      ContextSink(Context c, String[] argv) {
        this.ctx = c;
        this.argv = argv;
      }
    }

    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("context", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new ContextSink(context, argv);
      }
    });
    FlumeBuilder.setSinkFactory(sf);

    String spec = "context(123, test=\"foo\", try=456)";
    ContextSink ctxSnk = (ContextSink) FlumeBuilder.buildSink(new Context(),
        spec);
    assertEquals("123", ctxSnk.argv[0]);
    assertEquals("foo", ctxSnk.ctx.getValue("test"));
    assertEquals("456", ctxSnk.ctx.getValue("try"));
  }

  @Test
  public void testKWArgContextDeco() throws RecognitionException,
      FlumeSpecException {
    class ContextDeco extends EventSinkDecorator<EventSink> {
      public Context ctx;
      public String[] argv;

      ContextDeco(EventSink s, Context c, String[] argv) {
        super(s);
        this.ctx = c;
        this.argv = argv;
      }
    }

    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setDeco("context", new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        return new ContextDeco(null, context, argv);
      }
    });
    FlumeBuilder.setSinkFactory(sf);

    String spec = "{ context(123, test=\"foo\", try=456) => null }";
    ContextDeco ctxSnk = (ContextDeco) FlumeBuilder.buildSink(new Context(),
        spec);
    assertEquals("123", ctxSnk.argv[0]);
    assertEquals("foo", ctxSnk.ctx.getValue("test"));
    assertEquals("456", ctxSnk.ctx.getValue("try"));

  }

  @Test
  public void testSinkParser() throws RecognitionException {
    LOG.info("== sinks ==");

    String s = "text";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(s);
    LOG.info(toTree(o));
    assertEquals("(DECO (SINK text))", toTree(o));

    String s2 = "text(\"bogusdata\")";

    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(s2);
    LOG.info(toTree(o2));
    assertEquals("(DECO (SINK text (STRING \"bogusdata\")))", toTree(o2));

    String s3 = "[text(true)]";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(s3);
    LOG.info(toTree(o3));
    assertEquals("(MULTI (DECO (SINK text (BOOL true))))", toTree(o3));

    String s4 = "[text(true) , tail(\"somthingelse\") ]";
    Object o4 = FlumeBuilder.parseSink(s4);
    LOG.info(s4);
    LOG.info(toTree(o4));
    assertEquals(
        "(MULTI (DECO (SINK text (BOOL true))) (DECO (SINK tail (STRING \"somthingelse\"))))",
        toTree(o4));
  }

  @Test
  public void testGenSinks() throws RecognitionException {
    String s2 = "collector() { null }";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(s2);
    LOG.info(toTree(o2));
    assertEquals("(GEN collector (DECO (SINK null)))", toTree(o2));

  }

  @Test
  public void testBuilder() throws RecognitionException {
    LOG.info("== nodes ==");

    // String s = "source : tail(\"/var/log/httpd/access_log\") | thrift;";
    String s = "nodename : nodesource | nodesink;";
    Object o = FlumeBuilder.parse(s);
    LOG.info(toTree(o));
    assertEquals(
        "(NODE nodename (SOURCE nodesource) (DECO (SINK nodesink))) null",
        toTree(o));
  }

  @Test
  public void testDecorator() throws RecognitionException {
    LOG.info("== Decorators ==");

    String s = "{ deco => nodesink } ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(DECO (SINK deco) (DECO (SINK nodesink)))", toTree(o));

    String s3 = "{ deco1 => [ sink1, sink2] } ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals(
        "(DECO (SINK deco1) (MULTI (DECO (SINK sink1)) (DECO (SINK sink2))))",
        toTree(o3));

    // Test a "tight decorator" -- no extra spaces
    String s4 = "{deco1=>sink1} ";
    Object o4 = FlumeBuilder.parseSink(s4);
    LOG.info(toTree(o4));
    assertEquals("(DECO (SINK deco1) (DECO (SINK sink1)))", toTree(o4));
  }

  @Test
  public void testTerseDeco() throws RecognitionException {
    String multi2 = "flakeyAppend(.9,1337) console";
    Object o = FlumeBuilder.parseSink(multi2);
    LOG.info(toTree(o));

    assertEquals(
        "(DECO (SINK flakeyAppend (FLOAT .9) (DEC 1337)) (DECO (SINK console)))",
        toTree(o));

    String multi = "stubbornAppend flakeyAppend(.9,1337) console";
    Object o2 = FlumeBuilder.parseSink(multi);
    LOG.info(toTree(o2));

    assertEquals(
        "(DECO (SINK stubbornAppend) (DECO (SINK flakeyAppend (FLOAT .9) (DEC 1337)) (DECO (SINK console))))",
        toTree(o2));
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

  @Test
  public void testFailover() throws RecognitionException {
    LOG.info("== Failover ==");

    String s = "< deco ? nodesink > ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(BACKUP (DECO (SINK deco)) (DECO (SINK nodesink)))",
        toTree(o));

    String s2 = "<<  deco1 ? deco22 > ? nodesink > ";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(BACKUP (BACKUP (DECO (SINK deco1)) (DECO (SINK deco22))) (DECO (SINK nodesink)))",
        toTree(o2));

    String s3 = "< deco1 ? [ sink1, sink2] > ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals(
        "(BACKUP (DECO (SINK deco1)) (MULTI (DECO (SINK sink1)) (DECO (SINK sink2))))",
        toTree(o3));
  }

  @Test
  public void testRoll() throws RecognitionException {
    LOG.info("== roll == ");

    String s = "roll(100) { null }";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals("(ROLL (DECO (SINK null)) (DEC 100))", toTree(o));
  }

  @Test
  public void testCombo() throws RecognitionException {
    LOG.info("== Combo ==");

    String s = "< deco ? [nodesink, nodesink2] > ";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals(
        "(BACKUP (DECO (SINK deco)) (MULTI (DECO (SINK nodesink)) (DECO (SINK nodesink2))))",
        toTree(o));

    String s2 = "{ deco1 =>  <<  deco1 ? deco22 > ? nodesink > }";
    Object o2 = FlumeBuilder.parseSink(s2);
    LOG.info(toTree(o2));
    assertEquals(
        "(DECO (SINK deco1) (BACKUP (BACKUP (DECO (SINK deco1)) (DECO (SINK deco22))) (DECO (SINK nodesink))))",
        toTree(o2));

    String s3 = "< deco1 ? [ {sampler => sink1 } , sink2] > ";
    Object o3 = FlumeBuilder.parseSink(s3);
    LOG.info(toTree(o3));
    assertEquals(
        "(BACKUP (DECO (SINK deco1)) (MULTI (DECO (SINK sampler) (DECO (SINK sink1))) (DECO (SINK sink2))))",
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

  @Test
  public void testGenCollector() throws FlumeSpecException,
      RecognitionException {
    String s = "collector() { customDfsSink(\"file:///tmp/foo\",\"foo\") }";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals(
        "(GEN collector (DECO (SINK customDfsSink (STRING \"file:///tmp/foo\") (STRING \"foo\"))))",
        toTree(o));
  }

  @Test
  public void testFuncArgs() throws FlumeSpecException, RecognitionException {
    String s = "console(avro(\"snappy\"))";
    Object o = FlumeBuilder.parseSink(s);
    LOG.info(toTree(o));
    assertEquals(
        "(DECO (SINK console (FUNC avro (STRING \"snappy\"))))",
        toTree(o));
  }

}
