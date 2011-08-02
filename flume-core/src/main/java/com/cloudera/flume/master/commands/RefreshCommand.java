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

import java.io.IOException;

import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.Execable;
import com.cloudera.flume.master.FlumeMaster;
import com.google.common.base.Preconditions;

/**
 * This refreshes the data flow spec for a node by re-inserting it into the
 * ConfigManager. This is useful for updating failure chains when new collectors
 * are added, or just forcing a logical node to open and close.
 */
public class RefreshCommand {

  String node;

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  /**
   * Build a command for the command manager.
   */
  public Command toCommand() {
    return new Command("refresh", node);
  }

  /**
   * Actual Command execution.
   */
  public static Execable buildExecable() {
    return new Execable() {
      @Override
      public void exec(String[] argv) throws IOException {
        Preconditions.checkArgument(argv.length == 1);
        FlumeMaster master = FlumeMaster.getInstance();
        String node = argv[0];
        master.getSpecMan().refresh(node);
      }
    };
  }
}
