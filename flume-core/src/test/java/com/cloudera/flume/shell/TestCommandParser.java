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

package com.cloudera.flume.shell;

import static org.junit.Assert.assertEquals;

import org.antlr.runtime.RecognitionException;
import org.junit.Test;

import com.cloudera.flume.master.Command;

/**
 * Tests the flume command line parser.
 */
public class TestCommandParser {

  @Test
  public void testParseLine() throws RecognitionException {
    Command c1 = CommandBuilder.parseLine("config");
    assertEquals("config", c1.getCommand());
    assertEquals(0, c1.getArgs().length);

    Command c2 = CommandBuilder.parseLine("config this is a test");
    assertEquals("config", c2.getCommand());
    assertEquals("this", c2.getArgs()[0]);
    assertEquals("is", c2.getArgs()[1]);
    assertEquals("a", c2.getArgs()[2]);
    assertEquals("test", c2.getArgs()[3]);

    Command c3 = CommandBuilder.parseLine("config bla bla-3409h");
    assertEquals("config", c3.getCommand());
    assertEquals("bla", c3.getArgs()[0]);
    assertEquals("bla-3409h", c3.getArgs()[1]);

    // Make sure we parse things that start with numbers or acceptable symbols
    // as well
    Command c4 = CommandBuilder.parseLine("1 -2 :3 .4");
    assertEquals("1", c4.getCommand());
    assertEquals("-2", c4.getArgs()[0]);
    assertEquals(":3", c4.getArgs()[1]);
    assertEquals(".4", c4.getArgs()[2]);
  }

  @Test
  public void testDQuote() throws RecognitionException {
    Command c = CommandBuilder.parseLine("config bla \"foo\" sdlkjfs");
    assertEquals("config", c.getCommand());
    assertEquals("bla", c.getArgs()[0]);
    assertEquals("foo", c.getArgs()[1]);
    assertEquals("sdlkjfs", c.getArgs()[2]);

    c = CommandBuilder.parseLine("config \"foo blah bkur\"");
    assertEquals("config", c.getCommand());
    assertEquals("foo blah bkur", c.getArgs()[0]);

    c = CommandBuilder.parseLine("\"test\" \"test1\" foobard");
    assertEquals("test", c.getCommand());
    assertEquals("test1", c.getArgs()[0]);
    assertEquals("foobard", c.getArgs()[1]);

  }

  @Test
  public void testSQuote() throws RecognitionException {
    Command c = CommandBuilder.parseLine("config bla 'blah'");
    assertEquals("config", c.getCommand());
    assertEquals("bla", c.getArgs()[0]);
    assertEquals("blah", c.getArgs()[1]);

    c = CommandBuilder.parseLine("config '\"foo blah bkur\"'");
    assertEquals("config", c.getCommand());
    assertEquals("\"foo blah bkur\"", c.getArgs()[0]);
  }

  /**
   * Test with things commonly found in a flume dataflow spec.
   */
  @Test
  public void testFlumeSpec() throws RecognitionException {
    Command c = CommandBuilder
        .parseLine("config blitzwing 'console' 'console(\"avrojson\")'");
    assertEquals("config", c.getCommand());
    assertEquals("blitzwing", c.getArgs()[0]);
    assertEquals("console", c.getArgs()[1]);
    assertEquals("console(\"avrojson\")", c.getArgs()[2]);

    c = CommandBuilder
        .parseLine("multiconfig 'blitzwing : console | {delay(100) => console(\"avrojson\")};'");
    assertEquals("multiconfig", c.getCommand());
    assertEquals(
        "blitzwing : console | {delay(100) => console(\"avrojson\")};", c
            .getArgs()[0]);
  }

  @Test(expected = RecognitionException.class)
  public void testIllegalLex() throws RecognitionException {
    // missing a single quote
    CommandBuilder
        .parseLine("config blitzwing 'console' 'console(\"avrojson\")");

  }

  @Test(expected = RecognitionException.class)
  public void testIllegalLex2() throws RecognitionException {
    // not permitting some characters unless quoted.
    CommandBuilder
        .parseLine("config @blitzwing 'console' 'console(\"avrojson\")");

  }
}
