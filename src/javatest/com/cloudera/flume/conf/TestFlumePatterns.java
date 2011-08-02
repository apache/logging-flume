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

import static com.cloudera.flume.conf.FlumePatterns.deco;
import static com.cloudera.flume.conf.FlumePatterns.sink;
import static com.cloudera.flume.conf.FlumePatterns.sinkOnly;
import static com.cloudera.flume.conf.PatternMatch.recursive;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * This tests Patterns against the full suite of AST types.
 */
public class TestFlumePatterns {
  public static Logger LOG = Logger.getLogger(TestFlumePatterns.class);

  final static String megasink = "{ deco1 => { deco2 =>  "
      + "let letvar := test in [ < sink1 ? sink2> , sink3, { deco3 => sink4}  ]  } } ";
  final static String megasink2 = "failchain (\"foo\") { "
      + "{ deco1 => roll (200) { let letvar := test in "
      + "[ < sink1 ? sink2> , sink3, { deco3 => sink4}  ] } } }";

  static CommonTree parse = null;
  static CommonTree parse2 = null;
  static {
    try {
      parse = FlumeBuilder.parseSink(megasink);
      parse2 = FlumeBuilder.parseSink(megasink2);
    } catch (RecognitionException e) {
    }
  }

  @Before
  public void setDebug() {
    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  /**
   * Make sure the deco and sink patterns traverse to find the proper elements.
   */
  @Test
  public void testPatterns() {
    LOG.info(parse.toStringTree());
    assertNotNull(recursive(sink("sink4")).match(parse));
    assertNotNull(recursive(sinkOnly("sink4")).match(parse));

    assertNull(recursive(deco("sink4")).match(parse));
    assertNotNull(recursive(deco("deco1")).match(parse));
    assertNotNull(recursive(deco("deco2")).match(parse));
    assertNotNull(recursive(deco("deco3")).match(parse));
  }

  /**
   * Another sample with roll and failchain
   */
  @Test
  public void testPatterns2() {
    LOG.info(parse2.toStringTree());
    assertNotNull(recursive(sink("sink4")).match(parse2));
    assertNotNull(recursive(sinkOnly("sink4")).match(parse2));

    assertNull(recursive(deco("sink4")).match(parse2));
    assertNotNull(recursive(deco("deco1")).match(parse2));
    assertNotNull(recursive(deco("deco3")).match(parse2));
  }

}
