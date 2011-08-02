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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

/**
 * This test shows the friendlier messages for parser and lexer related
 * exception.
 */
public class TestParserErrorMessages {
  public static final Logger LOG = LoggerFactory.getLogger(TestParserErrorMessages.class);

  @Test
  public void testBadLexer() {
    try {
      FlumeBuilder.buildSink(new Context(), "n@ot");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      // this is a friendlier exception
      assertEquals("Lexer error at char '@' at line 1 char 1", e.getMessage());
    }
  }

  @Test
  public void testBadSourceValue() {
    try {
      FlumeBuilder.buildSource("foo");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      assertEquals("Invalid source: foo", e.getMessage());
    }
  }

  @Test
  public void testBadSinkValue() {
    try {
      FlumeBuilder.buildSink(new Context(), "{nullDeco => stest}");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      assertEquals("Invalid sink: stest", e.getMessage());
    }
  }

  @Test
  public void testBadSinkDecoValue() {
    try {
      FlumeBuilder.buildSink(new Context(), "{not(\"foo\") => stest}");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      assertEquals("Invalid sink decorator: not( \"foo\" )", e.getMessage());
    }
  }

  /**
   * the null sink takes no arguments.
   */
  @Test
  public void testIllegalArg1() {
    try {
      FlumeBuilder.buildSink(new Context(), "null(\"foo\")");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      // this is a friendlier exception
      assertEquals("usage: null", e.getMessage());
    }
  }

  /**
   * The arg is supposed to be a number
   */
  @Test
  public void testIllegalArg2() {
    try {
      FlumeBuilder.buildSink(new Context(), "{ delay(\"foo\") => null}");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      // this is a friendlier exception
      assertEquals("Illegal number format: For input string: \"foo\"", e
          .getMessage());
    }
  }

  /**
   * Certain parts of the parser are more flexible bug currently unsupported
   * during build
   * 
   * Example -- multiple retries, multiple decorators
   */
  @Test
  public void testValidParseIllegalBuild() {
    try {
      FlumeBuilder.buildSink(new Context(), "< null ? null ? null>");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      // this is a friendlier exception
      assertEquals("Only supports two retry nodes per failover expression", e
          .getMessage());
    }

    try {
      FlumeBuilder.buildSink(new Context(),
          "{ nullDeco =>  nullDeco =>  null }");
    } catch (FlumeSpecException e) {
      LOG.info(e.getMessage());
      // this is a friendlier exception
      assertEquals("Lexer error at char '0' at line 1 char 24", e.getMessage());
    }

  }
}
