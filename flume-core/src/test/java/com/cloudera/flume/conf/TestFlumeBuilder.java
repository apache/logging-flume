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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.collector.CollectorSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.rolling.RollSink;

/**
 * This code tests the parser and config spec error exceptions and data. Thses
 * should parse properly but fail becuase of invalid # of args, invalid types
 * for args, or invalid names for sources, sinks, and decorators.
 */
public class TestFlumeBuilder implements ExampleData {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestFlumeBuilder.class);
  String SOURCE = "text(\"bogusfile\")";

  @Test
  public void testBadParse() throws FlumeSpecException {
    try {
      FlumeBuilder.buildSink(new Context(), "asink [ something bad] ");
    } catch (Exception e) {
      // This is actually what happens
      System.out.println(e);
      return;
    }
    fail("expected recognition exception");
  }

  @Test
  public void testBadLex() throws FlumeSpecException {
    try {
      FlumeBuilder.buildSink(new Context(), "#$!@#$!@#$ ");
    } catch (Exception e) {
      // This is actually what happens
      System.out.println(e);
      return;
    }
    fail("expected recognition exception");
  }

  @Test
  public void testBuildConsole() throws IOException, FlumeSpecException {
    FlumeBuilder.buildSink(new Context(), "console");
  }

  @Test
  public void testBuildBadArgs() throws FlumeSpecException {
    try {
      // too many arguments
      FlumeBuilder.buildSink(new Context(), "console(1,2,3,4,5,6)");
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;// we expected this exception to be thrown.
    }
    fail("should have thrown exception");
  }

  @Test
  public void testBuildConsoleBad() {

    // too many arguments
    try {
      FlumeBuilder.buildSink(new Context(), "unpossiblesink");
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return; // we expected this exception to be thrown.
    }

    fail("should have thrown exception");
  }

  // Need to be able to handle FQDN names like www.foo.com, and
  // IPv4 addresses like 192.168.1.1
  @Test
  public void testHostnamne() throws org.antlr.runtime.RecognitionException {
    // simple
    CommonTree t = null;
    t = FlumeBuilder.parseHost("localhost");
    System.out.println(t);

    // fqdn
    t = FlumeBuilder.parseHost("localhost.localdomain.com");
    System.out.println(t);

    // ip adder
    t = FlumeBuilder.parseHost("1.2.3.4");
    System.out.println(t);

  }

  @Test
  public void testMultiSink() throws IOException, FlumeSpecException {
    String multi = "[ console , counter(\"count\") ]";
    FlumeBuilder.buildSink(new Context(), multi);
  }

  @Test
  public void testMultiSinkBad() throws IOException, FlumeSpecException {
    try {
      String multi = "[ console , unpossiblesink(\"count\") ]";
      FlumeBuilder.buildSink(new Context(), multi);
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;
    }
    fail("should have thrown exception");
  }

  @Test
  public void testDecorated() throws IOException, FlumeSpecException {
    String decorated = "{ intervalSampler(5) =>  console }";
    FlumeBuilder.buildSink(new Context(), decorated);

  }

  @Test
  public void testDecoratedBad1() throws IOException, FlumeSpecException {
    try {
      String decorated = "{ unpossible(5) =>  console }";
      FlumeBuilder.buildSink(new Context(), decorated);
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;
    }
    fail("should have thrown exception");
  }

  @Test
  public void testDecoratedBad2() throws IOException, FlumeSpecException {
    try {
      String decorated = "{ intervalSampler(5) =>  unpossible }";
      FlumeBuilder.buildSink(new Context(), decorated);
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;
    }
    fail("should have thrown exception");
  }

  /**
   * This is being ignored for the time being -- this is a future JIRA.
   */
  @Ignore
  @Test
  public void testDecoChain() throws RecognitionException {
    String decoChain = "{ nullDeco => nullDeco => nullDeco => null}";
    FlumeBuilder.parseSink(decoChain);
  }

  @Test
  public void testFailover() throws IOException, FlumeSpecException {
    String multi2 = "< flakeyAppend(.9,1337) console ? counter(\"count\") >";
    FlumeBuilder.buildSink(new Context(), multi2);

    String multi = "< { flakeyAppend(.9,1337) => console } ? counter(\"count\") >";
    FlumeBuilder.buildSink(new Context(), multi);
  }

  @Test
  public void testTerseDeco() throws IOException, FlumeSpecException {
    String multi2 = "flakeyAppend(.9,1337) console";
    FlumeBuilder.buildSink(new Context(), multi2);
  }

  @Test
  public void testFailoverBad1() throws IOException, FlumeSpecException {
    try {
      String multi = "<  unpossible ? counter(\"count\") >";
      FlumeBuilder.buildSink(new Context(), multi);
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;
    }
    fail("should have thrown exception");

  }

  @Test
  public void testFailoverBad2() throws IOException, FlumeSpecException {
    try {
      String multi = "< { flakeyAppend(.9,1337) => console } ? unpossible(\"count\") >";
      FlumeBuilder.buildSink(new Context(), multi);
    } catch (FlumeSpecException e) {
      System.out.println(e);
      return;
    }
    fail("should have thrown exception");

  }

  @Test
  public void testRollSinkParse() throws FlumeSpecException,
      RecognitionException {
    String roll = "roll (2123) { null } ";
    FlumeBuilder.parseSink(roll);
  }

  @Test
  public void testRollSink() throws FlumeSpecException, RecognitionException {
    String roll = "roll (200) { null } ";
    EventSink snk = FlumeBuilder.buildSink(new Context(), roll);
    assertTrue(snk instanceof RollSink);
  }

  /**
   * Make sure only simple "sink" things allowed in left hand side of decorator.
   * In this case , deco1 is ok, but the let statement makes no sense because it
   * ends up that there is a sink decorating a sink instead of a sink decorator
   * decorating a sink.
   */
  @Test(expected = RuntimeRecognitionException.class)
  public void testLetInWrongPlace() throws RecognitionException {
    String badsink = "{ deco1 => { let letvar := test in deco2 =>   [ < sink1 ? sink2> , sink3, { deco3 => sink4}  ]  } } ";
    CommonTree parse = FlumeBuilder.parseSink(badsink);
    LOG.info(parse.toStringTree());
  }

  /**
   * Verify basic functionality - contains basic sink/source/decorator
   */
  @Test
  public void testListExtensions() {
    FlumeBuilder.getSinkNames().contains("agentSink");
    FlumeBuilder.getDecoratorNames().contains("regex");
    FlumeBuilder.getSourceNames().contains("collectorSource");
  }

  @Test
  public void testGenCollector() throws FlumeSpecException {
    EventSink snk = FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "collector() { customDfsSink(\"file:///tmp/foo\",\"foo\") }");
    assertTrue(snk instanceof CollectorSink);
  }

  @Test
  public void testGenCollectorArgs() throws FlumeSpecException {
    EventSink snk = FlumeBuilder
        .buildSink(LogicalNodeContext.testingContext(),
            "collector(foo=\"bar\") { customDfsSink(\"file:///tmp/foo\",\"foo\") }");
    assertTrue(snk instanceof CollectorSink);

    snk = FlumeBuilder
        .buildSink(LogicalNodeContext.testingContext(),
            "collector(3,foo=\"bar\") { customDfsSink(\"file:///tmp/foo\",\"foo\") }");
    assertTrue(snk instanceof CollectorSink);

    snk = FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "collector(3) { customDfsSink(\"file:///tmp/foo\",\"foo\") }");
    assertTrue(snk instanceof CollectorSink);

  }
}
