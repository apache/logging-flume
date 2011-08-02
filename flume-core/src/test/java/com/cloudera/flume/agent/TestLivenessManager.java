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
package com.cloudera.flume.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.util.NetUtils;

/**
 * Test cases for the LivenessManager module.
 */
public class TestLivenessManager {

  FlumeMaster master = null;

  FlumeConfiguration cfg;

  @Before
  public void setCfg() throws IOException {
    // Isolate tests by only using simple cfg store
    cfg = FlumeConfiguration.createTestableConfiguration();
    cfg.set(FlumeConfiguration.MASTER_STORE, "memory");
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
  }

  @After
  public void shutdownMaster() {
    if (master != null) {
      master.shutdown();
      master = null;
    }
  }

  /**
   * This test check to make sure that long waiting closes do not hang the
   * heartbeating rpc calls from nodes. We test this by having another thread do
   * a series of reconfigurations that would block for >15s due to mulitple
   * closes of the 'hang' sink, and bailing out on the test if it takes >2s.
   */
  @Test
  public void testNoHang() throws IOException, InterruptedException,
      FlumeSpecException {
    // add a hanging sink.
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("hang", new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          @Override
          public void close() {
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

        };
      }

    });
    FlumeBuilder.setSinkFactory(sf);

    final FlumeMaster master = new FlumeMaster(cfg);
    MasterRPC rpc = new DirectMasterRPC(master);

    final FlumeNode node = new FlumeNode(rpc, false, false);
    // should have nothing.
    assertEquals(0, node.getLogicalNodeManager().getNodes().size());

    final CountDownLatch done = new CountDownLatch(1);
    new Thread() {
      public void run() {
        LivenessManager liveMan = node.getLivenessManager();
        try {
          // update config node to something that will be interrupted.
          master.getSpecMan().setConfig(NetUtils.localhost(), "flow",
              "asciisynth(0)", "hang");
          liveMan.heartbeatChecks();
          Thread.sleep(250);

          // update config node to something that will be interrupted.
          master.getSpecMan().setConfig(NetUtils.localhost(), "flow",
              "asciisynth(0)", "hang");
          liveMan.heartbeatChecks();
          Thread.sleep(250);

          // update config node to something that will be interrupted.
          master.getSpecMan().setConfig(NetUtils.localhost(), "flow",
              "asciisynth(0)", "hang");
          liveMan.heartbeatChecks();
          Thread.sleep(250);
        } catch (IOException e) {
          return; // fail
        } catch (FlumeSpecException e) {
          return; // fail
        } catch (InterruptedException e) {
          return; // fail
        }
        done.countDown();
      }
    }.start();

    // false means timeout
    assertTrue("close call hung the heartbeat", done.await(2000,
        TimeUnit.MILLISECONDS));

  }
}
