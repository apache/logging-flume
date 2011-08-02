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

import java.io.IOException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.google.common.base.Preconditions;

/**
 * JSP forms populate java bean. We want elements to be commands, so we take the
 * bean and have a toCommand method that creates a Command that can be submitted
 * to the CommandManager.
 * 
 * TODO (jon) rename to ConfigForm
 */
public class ConfigCommand {
  String node;
  String source;
  String sink;
  // Node as chosen from a drop-down box - use 'node' instead if specified
  String nodeChoice;

  public String getNode() {
    return (node == null) ? nodeChoice : node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getSink() {
    return sink;
  }

  public void setSink(String sink) {
    this.sink = sink;
  }

  public void setNodeChoice(String nodeChoice) {
    this.nodeChoice = nodeChoice;
  }

  /**
   * Build a command for the command manager.
   */
  public Command toCommand() {
    return new Command("config", node != null ? node : nodeChoice, source, sink);
  }

  /**
   * Actual Command execution.
   */
  public static Execable buildExecable() {
    return new Execable() {
      @Override
      public void exec(String[] argv) throws IOException {
        Preconditions.checkArgument(argv.length == 3 || argv.length == 4);
        FlumeMaster master = FlumeMaster.getInstance();
        try {
          if (argv.length == 3) {
            master.getSpecMan()
                .setConfig(argv[0],
                    FlumeConfiguration.get().getDefaultFlowName(), argv[1],
                    argv[2]);
          } else {
            master.getSpecMan().setConfig(argv[0], argv[1], argv[2], argv[3]);
          }

        } catch (FlumeSpecException e) {
          throw new IOException(e);
        }
      }
    };
  }
}
