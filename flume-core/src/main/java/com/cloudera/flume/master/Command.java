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

package com.cloudera.flume.master;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * This is a simple command that is fed to the Command Manager. This is an
 * abstraction layer that allows new commands to be plugged into the flume
 * Master. This allows for simplified web applications forms, cli based command
 * injection, or a simple rpc command mechanisms.
 * 
 * TODO (jon) rename GenericCommandForm
 */
public class Command {
  String command;
  String[] args;

  public Command(String command, String... args) {
    Preconditions.checkNotNull(command);
    this.command = command;

    // args is not null -- ... defaults to zero length array when ignored.
    this.args = args;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public String[] getArgs() {
    return args.clone();
  }

  @Override
  public String toString() {
    return command + " " + Arrays.toString(args);
  }

}
