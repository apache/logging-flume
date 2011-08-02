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

import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

/**
 * This creates patterns for flume-specific ast pattern matching.
 */
public class FlumePatterns {

  /**
   * Matches if current ast node is a source of kind kind.
   */
  public static PatternMatch source(String k) {
    return kind("SOURCE").child(kind(k));
  }

  /**
   * Matches if current ast node is a sink (not a deco) of kind kind.
   */
  public static PatternMatch sinkOnly(String k) {
    return kind("SINK").child(kind(k));
  }

  /**
   * Matches if the current ast node is a deco (not a sink) of kind kind.
   */
  public static PatternMatch deco(String k) {
    // Example Deco ASTs:
    // (DECO (SINK k xxx) ( DECO xxx xxx ) )
    // (DECO (SINK k ) ( MULTI xxx xxx ) )
    // (DECO (SINK k xxx) ( MULTI xxx xxx ) )

    return tuple(kind("DECO"), kind("SINK").child(kind(k)), var(k + "Child",
        or(kind("SINK"), kind("DECO"), kind("MULTI"), kind("BACKUP"),
            kind("LET"), kind("ROLL"), kind("FAILCHAIN"))));
  }

  /**
   * Matches if the current as node is a sink or deco of kind kind.
   */
  public static PatternMatch sink(String kind) {
    return or(sinkOnly(kind), deco(kind));
  }

  /**
   * Recursively finds a sink (only) of kind k.
   * 
   * TODO(jon) this is not completely strict but will work fine from parser
   * generated ASTs.
   */
  public static CommonTree findSink(String sink, String k)
      throws RecognitionException {
    CommonTree lsnkTree = FlumeBuilder.parseSink(sink);
    PatternMatch p = recursive(var("lsnk", kind("SINK").child(
        kind("logicalSink"))));
    Map<String, CommonTree> matches = p.match(lsnkTree);

    if (matches == null) {
      // do nothing,
      return lsnkTree;
    }
    return matches.get("lsnk");
  }

  /**
   * Recursively finds a sink of kind kind.
   * 
   * TODO(jon) currently we do not have source decorators, so it is not really
   * necessary for this to be recursive.
   */
  public static CommonTree findSource(String src, String kind)
      throws RecognitionException {
    PatternMatch p = recursive(var("src", source(kind)));
    CommonTree ctsrc = FlumeBuilder.parseSource(src);
    Map<String, CommonTree> matches = p.match(ctsrc);
    if (matches == null)
      return null;
    return matches.get("src");
  }
}
