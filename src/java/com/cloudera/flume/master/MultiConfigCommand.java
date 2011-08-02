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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.FlumeSpecGen;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.google.common.base.Preconditions;

/**
 * This is for setting multiple configurations at one time, from a text field.
 * It has the java bean interface for the jsp, and toCommand mechanism for the
 * command manager.
 * 
 * TODO (jon) rename to MultiConfigForm
 */
public class MultiConfigCommand {
  static final Logger LOG = LoggerFactory.getLogger(MultiConfigCommand.class);

  String specification;

  public String getSpecification() {
    return specification;
  }

  public void setSpecification(String specification) {
    this.specification = specification;
  }

  /**
   * Build a command for the command manager.
   */
  public Command toCommand() {
    return new Command("multiconfig", specification);
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
        boolean needsRefresh = false;
        try {
          List<FlumeNodeSpec> cfgs = FlumeSpecGen.generate(argv[0]);
          // check all cfgs to make sure they are valid
          for (FlumeNodeSpec spec : cfgs) {
            // TODO: first arg should be physical node
            Context ctx = new LogicalNodeContext(spec.node, spec.node);
            FlumeBuilder.buildSource(ctx, spec.src);
            FlumeBuilder.buildSink(ctx, spec.sink);
          }

          // set all cfgs to make sure they are valid.
          ConfigurationManager specman = master.getSpecMan();
          Map<String, FlumeConfigData> configs = new HashMap<String, FlumeConfigData>();
          for (FlumeNodeSpec spec : cfgs) {

            configs.put(spec.node, new FlumeConfigData(0, spec.src, spec.sink,
                LogicalNode.VERSION_INFIMUM, LogicalNode.VERSION_INFIMUM,
                FlumeConfiguration.get().getDefaultFlowName()));

            if (spec.src.contains("collectorSource")) {
              needsRefresh = true;
            }
          }

          specman.setBulkConfig(configs);

          if (needsRefresh) {
            specman.refreshAll();
          }

        } catch (FlumeSpecException e) {
          LOG.error("Invalid Flume specification", e);
          throw new IOException(e);
        }
      }
    };
  }
}
