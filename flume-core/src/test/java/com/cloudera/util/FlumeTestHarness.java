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
package com.cloudera.util;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LivenessManager;
import com.cloudera.flume.agent.LogicalNodeManager;
import com.cloudera.flume.agent.MockMasterRPC;
import com.cloudera.flume.agent.diskfailover.DiskFailoverManager;
import com.cloudera.flume.agent.diskfailover.NaiveFileFailoverManager;
import com.cloudera.flume.agent.durability.NaiveFileWALManager;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.reporter.ReportManager;

/**
 * This sets up a batttery of synthetic datasets for testing against different
 * decorators and sinks. Generally, each test requires ~2GB mem. ~1GB for
 * keeping a data set in memory and the other GB for some gc headroom.
 */
public class FlumeTestHarness {
  static final Logger LOG = LoggerFactory.getLogger(FlumeTestHarness.class);

  // These are setup to point to new default logging dir for each test.
  public static FlumeNode node;
  public static MockMasterRPC mock;
  public static File tmpdir;

  /**
   * This sets the log dir in the FlumeConfiguration and then instantiates a
   * mock master and node that use that configuration
   */
  public static void setupLocalWriteDir() {
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      Assert.fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.clear(); // reset all back to defaults.
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    mock = new MockMasterRPC();
    node = new FlumeNode(mock, false /* starthttp */, false /* oneshot */);
    ReportManager.get().clear();
  }

  /**
   * This version allows a particular test case to replace the default
   * xxxManager with one that is reasonable for the test.
   * 
   * Any args that are null will default to the "normal" version.
   */
  public static void setupFlumeNode(LogicalNodeManager nodesMan,
      WALManager walMan, DiskFailoverManager dfMan,
      CollectorAckListener colAck, LivenessManager liveman) {
    try {
      tmpdir = FileUtil.mktempdir();
    } catch (Exception e) {
      Assert.fail("mk temp dir failed");
    }
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.AGENT_LOG_DIR_NEW, tmpdir.getAbsolutePath());

    mock = new MockMasterRPC();

    nodesMan = (nodesMan != null) ? nodesMan : new LogicalNodeManager(NetUtils
        .localhost());
    walMan = (walMan != null) ? walMan : new NaiveFileWALManager(new File(conf
        .getAgentLogsDir()));
    dfMan = (dfMan != null) ? dfMan : new NaiveFileFailoverManager(new File(
        conf.getAgentLogsDir()));
    colAck = (colAck != null) ? colAck : new CollectorAckListener(mock);
    liveman = (liveman != null) ? liveman : new LivenessManager(nodesMan, mock,
        walMan);

    node = new FlumeNode(NetUtils.localhost(), mock, nodesMan, walMan, dfMan,
        colAck, liveman);
  }

  /**
   * Cleanup the temp dir after the test is run.
   */
  public static void cleanupLocalWriteDir() throws IOException {
    FileUtil.rmr(tmpdir);
  }


}
