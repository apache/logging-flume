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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Save all configs to a file.
 */
public class SaveConfigCommand {

  static final Logger LOG = LoggerFactory.getLogger(SaveConfigCommand.class);

  /**
   * Actual Command execution.
   */
  public static Execable buildExecable() {
    return new Execable() {
      @Override
      public void exec(String[] argv) throws IOException {
        Preconditions.checkArgument(argv.length == 1);
        String configFileName = argv[0];
        FlumeMaster master = FlumeMaster.getInstance();
        try {
          master.getSpecMan().saveConfigFile(configFileName);
        } catch (IOException e) {
          LOG.error("Save Config " + configFileName + " failed", e);
          throw e;
        }
      }
    };
  }

}
