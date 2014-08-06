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
package org.apache.flume.test.agent;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.flume.test.util.StagedInstall;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;

public class TestFileChannel {

  private static final Logger LOGGER = Logger.getLogger(TestFileChannel.class);

  private static final Collection<File> tempResources = new ArrayList<File>();

  private Properties agentProps;
  private File sinkOutputDir;

  @Before
  public void setUp() throws Exception {
      /* Create 3 temp dirs, each used as value within agentProps */

      final File sinkOutputDir = Files.createTempDir();
      tempResources.add(sinkOutputDir);
      final String sinkOutputDirPath = sinkOutputDir.getCanonicalPath();
      LOGGER.info("Created rolling file sink's output dir: "
              + sinkOutputDirPath);

      final File channelCheckpointDir = Files.createTempDir();
      tempResources.add(channelCheckpointDir);
      final String channelCheckpointDirPath = channelCheckpointDir
              .getCanonicalPath();
      LOGGER.info("Created file channel's checkpoint dir: "
              + channelCheckpointDirPath);

      final File channelDataDir = Files.createTempDir();
      tempResources.add(channelDataDir);
      final String channelDataDirPath = channelDataDir.getCanonicalPath();
      LOGGER.info("Created file channel's data dir: "
              + channelDataDirPath);

      /* Build props to pass to flume agent */

      Properties agentProps = new Properties();

      // Active sets
      agentProps.put("a1.channels", "c1");
      agentProps.put("a1.sources", "r1");
      agentProps.put("a1.sinks", "k1");

      // c1
      agentProps.put("a1.channels.c1.type", "FILE");
      agentProps.put("a1.channels.c1.checkpointDir", channelCheckpointDirPath);
      agentProps.put("a1.channels.c1.dataDirs", channelDataDirPath);

      // r1
      agentProps.put("a1.sources.r1.channels", "c1");
      agentProps.put("a1.sources.r1.type", "EXEC");
      agentProps.put("a1.sources.r1.command", "seq 1 100");

      // k1
      agentProps.put("a1.sinks.k1.channel", "c1");
      agentProps.put("a1.sinks.k1.type", "FILE_ROLL");
      agentProps.put("a1.sinks.k1.sink.directory", sinkOutputDirPath);
      agentProps.put("a1.sinks.k1.sink.rollInterval", "0");

      this.agentProps = agentProps;
      this.sinkOutputDir = sinkOutputDir;
  }

  @After
  public void tearDown() throws Exception {
    StagedInstall.getInstance().stopAgent();
    for (File tempResource : tempResources) {
        tempResource.delete();
    }
    agentProps = null;
  }

  /**
   * File channel in/out test. Verifies that all events inserted into the
   * file channel are received by the sink in order.
   *
   * The EXEC source creates 100 events where the event bodies have
   * sequential numbers. The source puts those events into the file channel,
   * and the FILE_ROLL The sink is expected to take all 100 events in FIFO
   * order.
   *
   * @throws Exception
   */
  @Test
   public void testInOut() throws Exception {
      LOGGER.debug("testInOut() started.");

      StagedInstall.getInstance().startAgent("a1", agentProps);
      TimeUnit.SECONDS.sleep(10); // Wait for source and sink to finish
                                  // TODO make this more deterministic

      /* Create expected output */

      StringBuffer sb = new StringBuffer();
      for (int i = 1; i <= 100; i++) {
          sb.append(i).append("\n");
      }
      String expectedOutput = sb.toString();
      LOGGER.info("Created expected output: " + expectedOutput);

      /* Create actual output file */

      File[] sinkOutputDirChildren = sinkOutputDir.listFiles();
      // Only 1 file should be in FILE_ROLL sink's dir (rolling is disabled)
      Assert.assertEquals("Expected FILE_ROLL sink's dir to have only 1 child," +
              " but found " + sinkOutputDirChildren.length + " children.",
              1, sinkOutputDirChildren.length);
      File actualOutput = sinkOutputDirChildren[0];

      if (!Files.toString(actualOutput, Charsets.UTF_8).equals(expectedOutput)) {
          LOGGER.error("Actual output doesn't match expected output.\n");
          throw new AssertionError("FILE_ROLL sink's actual output doesn't " +
                  "match expected output.");
      }

      LOGGER.debug("testInOut() ended.");
  }

}
