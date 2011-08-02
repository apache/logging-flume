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
package com.cloudera.flume.master.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.cloudera.flume.master.Command;

/**
 * This is a bean container for the hidden command interface on the ghetto
 * master webpage.
 */
public class GenericCommand {
  String cmd;
  String args;

  public String getCmd() {
    return cmd;
  }

  public void setCmd(String cmd) {
    this.cmd = cmd;
  }

  public String getArgs() {
    return args;
  }

  public void setArgs(String args) {
    this.args = args;
  }

  /**
   * Converts the bean arguments to the Command format which requires a command
   * and an array of strings.
   */
  public Command toCommand() {

    // TODO (jon) This is imperfect -- will only be tokenizing for first cut,
    // the right thing is to parse and handle quote escapes. This version will
    // improperly handle arguments with spaces in them. (they will be tokenized
    // when what people generally want is the ability to escape them.

    // just pass empty list of no args.
    List<String> list = new ArrayList<String>();
    if (args != null) {
      StringTokenizer tok = new StringTokenizer(args);
      while (tok.hasMoreTokens()) {
        list.add(tok.nextToken());
      }
    }

    String[] strArgs = list.toArray(new String[0]);
    return new Command(cmd, strArgs);

  }

}
