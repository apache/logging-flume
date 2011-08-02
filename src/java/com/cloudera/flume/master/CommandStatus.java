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

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.base.Preconditions;

/**
 * This keeps a command it runtime information about the command. Think of it
 * like a process control block in a OS.
 * 
 * The command can be in 4 states. QUEUED means the command has yet to be
 * executed. EXECING means it is currently being executed. SUCCEEDED means the
 * command ran successfully, while FAILED means the command ran and threw some
 * sort of exception.
 * 
 * The message field and arg in the state transitions can be used to provide
 * extra information about how/why the transition happened. This is useful for
 * conveying failure reason.
 */
@XmlRootElement
public class CommandStatus {
  public static enum State {
    QUEUED, EXECING, SUCCEEDED, FAILED
  };

  long cmdId; // uniq id for command. Used to check status of a command.
  Command cmd;

  State curState;
  String message; // this for extra information like why something failed.

  public CommandStatus(long cmdId, Command cmd, State state, String msg) {
    this.cmdId = cmdId;
    this.cmd = cmd;
    curState = state;
    message = msg;
  }

  /**
   * Empty constructor for rpc/jaxb
   */
  public CommandStatus() {
  }

  static CommandStatus createCommandStatus(long cmdId, Command cmd) {
    return new CommandStatus(cmdId, cmd, State.QUEUED, "");
  }

  public long getCmdID() {
    return cmdId;
  }

  public State getState() {
    return curState;
  }

  synchronized public void toExecing(String msg) {
    Preconditions.checkState(curState == State.QUEUED);
    curState = State.EXECING;
    message = msg;
  }

  synchronized public void toSucceeded(String msg) {
    Preconditions.checkState(curState == State.EXECING);
    curState = State.SUCCEEDED;
    message = msg;
  }

  synchronized public void toFailed(String msg) {
    Preconditions.checkState(curState == State.EXECING);
    curState = State.FAILED;
    message = msg;
  }

  public boolean isSuccess() {
    return curState == State.SUCCEEDED;
  }

  public boolean isFailure() {
    return curState == State.FAILED;
  }

  public boolean isQueued() {
    return curState == State.QUEUED;
  }

  public boolean isInProgress() {
    return curState == State.EXECING;
  }

  public Command getCommand() {
    return cmd;
  }

  public String getMessage() {
    return message;
  }

  public String toString() {
    return "cmdid:" + cmdId + " " + getCommand().toString();
  }

}
