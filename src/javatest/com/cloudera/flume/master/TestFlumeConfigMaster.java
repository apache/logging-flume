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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.util.FileUtil;

/**
 * This test tests the load and save configuration mechanism.
 */

public class TestFlumeConfigMaster {

  public static Logger LOG = Logger.getLogger(TestFlumeConfigMaster.class);

  File tmpdir = null;

  FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();

  @Before
  public void setConfiguration() throws IOException {
    // Set directory of webapps to build-specific dir
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
    // Give ZK a temporary directory, otherwise it's possible we'll reload some
    // old configs
    tmpdir = FileUtil.mktempdir();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmpdir.getAbsolutePath());

    Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @After
  public void deleteConfigDir() throws IOException {
    if (tmpdir != null) {
      FileUtil.rmr(tmpdir);
      tmpdir = null;
    }
  }

  /**
   * Set a config, save it, overwrite the config, check it, load the original,
   * then make sure it is the same.
   */
  @Test
  public void testMasterSaveLoad() throws IOException, FlumeSpecException {

    // load a config and save
    FlumeMaster fcm = new FlumeMaster(cfg, false);

    fcm.serve();
    ConfigurationManager cm = fcm.getSpecMan();
    cm.setConfig("test1", "test-flow", "console", "console");
    File f = File.createTempFile("test", ".tmp");
    f.deleteOnExit();
    cm.saveConfig(f.getAbsolutePath());
    assertTrue(f.exists());
    assertTrue(f.length() > 0);

    FlumeConfigData data = cm.getConfig("test1");
    assertEquals(data.sinkConfig, "console");
    assertEquals(data.sourceConfig, "console");

    // overwrite previous config.
    cm.setConfig("test1", "test-flow", "null", "null");
    data = cm.getConfig("test1");
    assertEquals(data.sinkConfig, "null");
    assertEquals(data.sourceConfig, "null");

    // reload and make sure we had the first config.
    cm.loadConfig(f.getAbsolutePath());
    data = cm.getConfig("test1");
    assertEquals(data.sinkConfig, "console");
    assertEquals(data.sourceConfig, "console");
    fcm.shutdown();

  }

  @Test
  public void testOpenClose() throws TTransportException, IOException {

    FlumeMaster fm = new FlumeMaster(new CommandManager(), new ConfigManager(),
        new StatusManager(), new MasterAckManager(), cfg);

    for (int i = 0; i < 10; i++) {
      LOG.info("flume master open close " + i);
      fm.serve();

      fm.shutdown();
    }
  }

  /**
   * Test open and close with gossip ack manager being used
   */
  @Test
  public void testGossipOpenClose() throws TTransportException, IOException {
    FlumeMaster fm = new FlumeMaster(cfg, false);

    // TODO (henry) make the GossipAckManager shutdown cleanly.
    for (int i = 0; i < 10; i++) {
      LOG.info("flume master open close " + i);
      fm.serve();

      fm.shutdown();
    }
  }
}
