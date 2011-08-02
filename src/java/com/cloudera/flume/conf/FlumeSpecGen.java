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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

import com.cloudera.flume.conf.FlumeBuilder.ASTNODE;
import com.cloudera.flume.master.FlumeNodeSpec;
import com.google.common.base.Preconditions;

/**
 * This regenerates flume specs from parsed ASTs.
 * 
 * The master node reads and parses commands. If they parse cleanly, this is
 * used to regenerated the code as a string so it can be sent to the nodes.
 */
public class FlumeSpecGen {

  static String genArg(CommonTree t) throws FlumeSpecException {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case HEX:
    case DEC:
    case BOOL:
    case OCT:
    case STRING:
    case FLOAT:
      return t.getChild(0).getText();
    case KWARG:
      return t.getChild(0).getText() + "=" + genArg((CommonTree) t.getChild(1));
    default:
      throw new FlumeSpecException("Not a node of literal type: "
          + t.toStringTree());
    }
  }

  static String genArgs(List<String> args, String pre, String delim, String post) {
    StringBuilder b = new StringBuilder();
    if (args.size() == 0)
      return "";

    b.append(pre + " ");
    boolean first = true;
    for (String arg : args) {
      if (!first) {
        b.append(delim + " ");
      }
      b.append(arg);
      first = false;
    }
    b.append(" " + post);
    return b.toString();
  }

  @SuppressWarnings("unchecked")
  public static String genEventSource(CommonTree t) throws FlumeSpecException {
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case SOURCE:
      List<CommonTree> children = new ArrayList<CommonTree>(
          (List<CommonTree>) t.getChildren());
      String sourceType = children.remove(0).getText();
      List<String> args = new ArrayList<String>();
      for (CommonTree tr : children) {
        args.add(genArg(tr));
      }

      return sourceType + genArgs(args, "(", ",", ")");

    case MULTI:
      List<CommonTree> elems = (List<CommonTree>) t.getChildren();
      List<String> srcs = new ArrayList<String>();
      for (CommonTree tr : elems) {
        String src = genEventSource(tr);
        srcs.add(src);
      }
      return genArgs(srcs, "[", ",", "]");

    default:
      throw new FlumeSpecException("bad parse tree! Expected source but got "
          + t.toStringTree());
    }
  }

  @SuppressWarnings("unchecked")
  public static String genEventSink(CommonTree t) throws FlumeSpecException {
    if (t == null) {
      throw new FlumeSpecException("Tree is null");
    }
    ASTNODE type = ASTNODE.valueOf(t.getText()); // convert to enum
    switch (type) {
    case SINK:
      // copy so we don't modify original
      List<CommonTree> children = (List<CommonTree>) new ArrayList<CommonTree>(
          t.getChildren());
      String sinkType = children.remove(0).getText();
      List<String> args = new ArrayList<String>();
      for (CommonTree tr : children) {
        args.add(genArg(tr));
      }
      return sinkType + genArgs(args, "(", ",", ")");

    case MULTI:
      List<CommonTree> elems = (List<CommonTree>) t.getChildren();
      List<String> snks = new ArrayList<String>();
      for (CommonTree tr : elems) {
        String snk = genEventSink(tr);
        snks.add(snk);
      }

      return genArgs(snks, "[", ",", "]");

    case DECO: {
      List<CommonTree> decoNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(decoNodes.size() == 2
          || decoNodes.size() == 1);
      if (decoNodes.size() == 1) {
        CommonTree snk = decoNodes.get(0);
        return genEventSink(snk);
      }
      CommonTree deco = decoNodes.get(0);
      CommonTree decoSnk = decoNodes.get(1);
      String decoSink = genEventSinkDecorator(deco);
      String dsnk = genEventSink(decoSnk);
      return "{ " + decoSink + " => " + dsnk + " }";
    }

    case BACKUP: {
      List<CommonTree> backupNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(backupNodes.size() == 2);
      CommonTree main = backupNodes.get(0);
      CommonTree backup = backupNodes.get(1);
      String mainSink = genEventSink(main);
      String backupSink = genEventSink(backup);
      return "< " + mainSink + " ? " + backupSink + " >";
    }

    case LET: {
      List<CommonTree> backupNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(backupNodes.size() == 3);
      CommonTree name = backupNodes.get(0);
      CommonTree sub = backupNodes.get(1);
      CommonTree body = backupNodes.get(2);
      String argSink = genEventSink(sub);
      String bodySink = genEventSink(body);
      return "let " + name.getText() + " := " + argSink + " in " + bodySink;
    }

    case ROLL: {
      List<CommonTree> rollNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(rollNodes.size() == 2);
      CommonTree body = rollNodes.get(0);
      CommonTree arg = rollNodes.get(1);
      String bodySink = genEventSink(body);
      List<String> rargs = new ArrayList<String>();
      rargs.add(genArg(arg));
      String argSink = genArgs(rargs, "(", ",", ")");

      return "roll" + argSink + " { " + bodySink + " }";

    }

    case FAILCHAIN: {
      // TODO (jon) This is no longer necessary with the substitution mechanisms
      // found in the translators
      List<CommonTree> failNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(failNodes.size() >= 2);
      CommonTree body = failNodes.get(0);
      String bodySink = genEventSink(body);

      List<String> rargs = new ArrayList<String>();
      for (int i = 1; i < failNodes.size(); i++) {
        CommonTree arg = failNodes.get(i);
        rargs.add(genArg(arg));
      }
      String argSink = genArgs(rargs, "(", ",", ")");

      return "failchain" + argSink + " { " + bodySink + " }";

    }

    case GEN: {
      List<CommonTree> genNodes = (List<CommonTree>) t.getChildren();
      Preconditions.checkArgument(genNodes.size() >= 2);
      String genType = genNodes.get(0).getText();
      CommonTree body = genNodes.get(1);
      String bodySink = genEventSink(body);

      List<String> rargs = new ArrayList<String>();
      for (int i = 2; i < genNodes.size(); i++) {
        CommonTree arg = genNodes.get(i);
        rargs.add(genArg(arg));
      }
      String argSink = genArgs(rargs, "(", ",", ")");

      return genType + argSink + " { " + bodySink + " }";

    }
      // TODO (jon) handle pattern match splitter

    default:
      throw new FlumeSpecException("bad parse tree! expected sink but got "
          + t.toStringTree());
    }
  }

  @SuppressWarnings("unchecked")
  static String genEventSinkDecorator(CommonTree t) throws FlumeSpecException {
    List<CommonTree> children = new ArrayList<CommonTree>((List<CommonTree>) t
        .getChildren());
    String sinkType = children.remove(0).getText();
    List<String> args = new ArrayList<String>();
    for (CommonTree tr : children) {
      args.add(genArg(tr));
    }
    return sinkType + genArgs(args, "(", ",", ")");
    // sinkFactory.getDecorator(sinkType, args.toArray(new String[0]));
  }

  @SuppressWarnings("unchecked")
  public static List<FlumeNodeSpec> generate(String s)
      throws FlumeSpecException {
    try {
      CommonTree node = FlumeBuilder.parse(s);
      List<FlumeNodeSpec> cfg = new ArrayList<FlumeNodeSpec>();
      if (node.getChildren() == null) {
        return cfg;
      }
      for (CommonTree t : (List<CommonTree>) node.getChildren()) {
        // -1 is magic EOF value
        if (t.getType() == -1) {
          break; // I am done.
        }
        if (t.getText() != "NODE") {
          throw new FlumeSpecException("fail, expected node but had "
              + t.toStringTree());
        }

        String host = t.getChild(0).getText();
        CommonTree tsrc = (CommonTree) t.getChild(1);
        String src = genEventSource(tsrc);
        CommonTree tsnk = (CommonTree) t.getChild(2);
        String snk = genEventSink(tsnk);
        FlumeNodeSpec entry = new FlumeNodeSpec(host, src, snk);
        cfg.add(entry);
      }
      return cfg;
    } catch (RecognitionException re) {
      throw new FlumeSpecException(re.getMessage());
    } catch (RuntimeException re) {
      // catch other parse and lex errors
      throw new FlumeSpecException(re.getMessage());
    }
  }
}
