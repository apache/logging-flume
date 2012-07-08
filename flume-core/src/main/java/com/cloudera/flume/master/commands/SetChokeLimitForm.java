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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.Execable;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.MasterExecException;
import com.google.common.base.Preconditions;

/**
 * This implements the "setChokeLimit" command
 */
public class SetChokeLimitForm {
  static final Logger LOG = LoggerFactory.getLogger(SetChokeLimitForm.class);
  String logicalNode;

  public String getLogicalNode() {
    return logicalNode;
  }

  public void setLogicalNode(String logicalNode) {
    this.logicalNode = logicalNode;
  }

  /**
   * Convert this bean into a command.
   */
  public Command toCommand() {
    String[] args = { "physicalnode", "chokeID", "limit" };
    return new Command("setChokeLimit", args);
  }

  /**
   * Build an execable that will execute the command.
   */
  public static Execable buildExecable() {
    return new Execable() {
      // TODO(Vibhor): Once the PhysicalNodeThrottling patch is in, change the
      // preconditions here and add the ability to get the physicalNode
      // throttling limit.
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        // first check the length of the arguments
        Preconditions.checkArgument(args.length > 2,
            "Usage: setChokeLimit physicalNode chokeID limit");

        String physicalNodeName = args[0];
        // issue a polite warning if the physicalnode does not exist yet
        if (FlumeMaster.getInstance().getSpecMan().getLogicalNode(
            physicalNodeName).isEmpty()) {
          LOG.warn("PhysicalNode: " + physicalNodeName + " not present yet!");
        }

        int limit = 0;
        String chokerName = args[1];
        try {
          limit = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
          LOG.error("Limit not given in the right format");
          throw new MasterExecException("Limit not given in the right format",
              e);
        }

        Preconditions.checkState(limit >= 0, "Limit has to be at least 0");
        // only works in memory!! not in zookeeper.
        FlumeMaster.getInstance().getSpecMan().addChokeLimit(physicalNodeName,
            chokerName, limit);
      }
    };
  }
}
