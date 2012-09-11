/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import static org.apache.flume.channel.file.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TestFileChannelRestart extends TestFileChannelBase {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannelRestart.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
  }
  @Test
  public void testRestartLogReplayV1() throws Exception {
    doTestRestart(true, false, false, false);
  }
  @Test
  public void testRestartLogReplayV2() throws Exception {
    doTestRestart(false, false, false, false);
  }

  @Test
  public void testFastReplayV1() throws Exception {
    doTestRestart(true, true, true, true);
  }

  @Test
  public void testFastReplayV2() throws Exception {
    doTestRestart(false, true, true, true);
  }

  @Test
  public void testNormalReplayV1() throws Exception {
    doTestRestart(true, true, true, false);
  }

  @Test
  public void testNormalReplayV2() throws Exception {
    doTestRestart(false, true, true, false);
  }

  public void doTestRestart(boolean useLogReplayV1,
          boolean forceCheckpoint, boolean deleteCheckpoint,
          boolean useFastReplay) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_LOG_REPLAY_V1,
            String.valueOf(useLogReplayV1));
    overrides.put(
            FileChannelConfiguration.USE_FAST_REPLAY,
            String.valueOf(useFastReplay));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    if (forceCheckpoint) {
      forceCheckpoint(channel);
    }
    channel.stop();
    if(deleteCheckpoint) {
      File checkpoint = new File(checkpointDir, "checkpoint");
      Assert.assertTrue(checkpoint.delete());
      File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
      Assert.assertTrue(checkpointMetaData.delete());
    }
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testRestartFailsWhenMetaDataExistsButCheckpointDoesNot()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertEquals(1,  putEvents(channel, "restart", 1, 1).size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    Assert.assertTrue(checkpoint.delete());
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
  @Test
  public void testRestartFailsWhenCheckpointExistsButMetaDoesNot()
      throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Assert.assertEquals(1,  putEvents(channel, "restart", 1, 1).size());
    forceCheckpoint(channel);
    channel.stop();
    File checkpoint = new File(checkpointDir, "checkpoint");
    File checkpointMetaData = Serialization.getMetaDataFile(checkpoint);
    Assert.assertTrue(checkpointMetaData.delete());
    Assert.assertTrue(checkpoint.exists());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
}
