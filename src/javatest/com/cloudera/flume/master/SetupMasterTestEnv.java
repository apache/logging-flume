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

import java.io.File;
import java.io.IOException;

import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.FileUtil;

/**
 * A base class for tests, this sets up a master before the test, and tears it
 * down after.
 * 
 */
public class SetupMasterTestEnv {

  protected FlumeMaster flumeMaster = null;
  private File tmpdir = null;

  @Before
  public void setCfgAndStartMaster() throws TTransportException, IOException {
    // Give ZK a temporary directory, otherwise it's possible we'll reload some
    // old configs
    tmpdir = FileUtil.mktempdir();
    FlumeConfiguration.createTestableConfiguration();
    FlumeConfiguration.get().set(FlumeConfiguration.WEBAPPS_PATH,
        "build/webapps");
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_ZK_LOGDIR,
        tmpdir.getAbsolutePath());
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_STORE, "memory");
    flumeMaster = new FlumeMaster(new CommandManager(), new ConfigManager(),
        new StatusManager(), new MasterAckManager(), FlumeConfiguration.get());
    flumeMaster.serve();
  }

  @After
  public void stopMaster() throws IOException {
    if (flumeMaster != null) {
      flumeMaster.shutdown();
      flumeMaster = null;
    }

    if (tmpdir != null) {
      FileUtil.rmr(tmpdir);
      tmpdir = null;
    }
  }

}
