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

import java.util.ArrayList;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.master.Command;
import com.cloudera.flume.shell.antlr.FlumeShellLexer;
import com.cloudera.flume.shell.antlr.FlumeShellParser;

/**
 * Builds a command object from a string. Needed for sane command line parsing.
 * 
 * Unquoted tokens can contain alphanumeric, '.',':','_', or '-'. Tokens
 * enclosed in '"' will be java string unescaped. Tokens enclosed in ''' (single
 * quotes) are not unescaped at all and can contain any char except for '''.
 * Exceptions are thrown if quotes are not properly matched or invalid chars
 * present in unquoted tokens. .
 */
public class CommandBuilder {
  public static final Logger LOG = LoggerFactory.getLogger(CommandBuilder.class);

  enum ASTNODE {
    CMD, DQUOTE, SQUOTE, STRING
  };

  /**
   * This hooks a particular string to the lexer. From there it creates a parser
   * that can be started from different entities. The lexer and language are
   * case sensitive.
   */
  static FlumeShellParser getShellCmdParser(String s) {
    FlumeShellLexer lexer = new FlumeShellLexer(new ANTLRStringStream(s));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    return new FlumeShellParser(tokens);
  }

  static CommonTokenStream getTokenStream(String s) {
    FlumeShellLexer lexer = new FlumeShellLexer(new ANTLRStringStream(s));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    return tokens;
  }

  static String toString(CommonTree literal) {
    ASTNODE type = ASTNODE.valueOf(literal.getText());
    switch (type) {
    case SQUOTE:
      // remove single quotes
      String sq = literal.getChild(0).getText();
      sq = sq.substring(1, sq.length() - 1);
      return sq;

    case DQUOTE:
      // remove double quotes and unescape
      String dq = literal.getChild(0).getText();
      dq = dq.substring(1, dq.length() - 1);
      return StringEscapeUtils.unescapeJava(dq);

    case STRING:
      // just return string
      return literal.getChild(0).getText();
    default:
      throw new IllegalStateException("illegal parse!");
    }
  }

  /**
   * This takes a single string and parses it into a Command.
   */
  public static Command parseLine(String s) throws RecognitionException {
    try {
      CommonTree cmd = (CommonTree) getShellCmdParser(s).line().getTree();
      CommonTree ast = (CommonTree) cmd.getChild(0);
      String command = toString(ast);
      cmd.deleteChild(0);
      ArrayList<String> lst = new ArrayList<String>();
      for (int i = 0; i < cmd.getChildCount(); i++) {
        String tok = toString((CommonTree) cmd.getChild(i));
        lst.add(tok);
      }
      return new Command(command, lst.toArray(new String[0]));

    } catch (RecognitionException e) {
      LOG.debug("Failed to parse line, '" + s + "' because of "
          + e.getMessage(), e);
      throw e;
    } catch (RuntimeException rex) {
      // right now lexer errors are RTE's
      LOG
          .debug("Failed to lex '" + s + "' because of " + rex.getMessage(),
              rex);
      throw new CommandLineException(rex);
    }
  }
}
