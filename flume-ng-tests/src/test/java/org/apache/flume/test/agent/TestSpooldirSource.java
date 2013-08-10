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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.flume.test.util.StagedInstall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSpooldirSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestSpooldirSource.class);

  private Properties agentProps;
  private File sinkOutputDir;
  private List<File> spoolDirs = Lists.newArrayList();

  @Before
  public void setup() throws Exception {

    File agentDir = StagedInstall.getInstance().getStageDir();
    LOGGER.debug("Using agent stage dir: {}", agentDir);

    File testDir = new File(agentDir, TestSpooldirSource.class.getName());
    assertTrue(testDir.mkdirs());

    File spoolParentDir = new File(testDir, "spools");
    assertTrue("Unable to create sink output dir: " + spoolParentDir.getPath(),
        spoolParentDir.mkdir());

    final int NUM_SOURCES = 100;

    agentProps = new Properties();
    List<String> spooldirSrcNames = Lists.newArrayList();
    String channelName = "mem-01";

    // Create source dirs and property file chunks
    for (int i = 0; i < NUM_SOURCES; i++) {
      String srcName = String.format("spooldir-%03d", i);
      File spoolDir = new File(spoolParentDir, srcName);
      assertTrue(spoolDir.mkdir());
      spooldirSrcNames.add(srcName);
      spoolDirs.add(spoolDir);

      agentProps.put(String.format("agent.sources.%s.type", srcName),
          "SPOOLDIR");
      agentProps.put(String.format("agent.sources.%s.spoolDir", srcName),
          spoolDir.getPath());
      agentProps.put(String.format("agent.sources.%s.channels", srcName),
          channelName);
    }

    // Create the rest of the properties file
    agentProps.put("agent.channels.mem-01.type", "MEMORY");
    agentProps.put("agent.channels.mem-01.capacity", String.valueOf(100000));

    sinkOutputDir = new File(testDir, "out");
    assertTrue("Unable to create sink output dir: " + sinkOutputDir.getPath(),
        sinkOutputDir.mkdir());

    agentProps.put("agent.sinks.roll-01.channel", channelName);
    agentProps.put("agent.sinks.roll-01.type", "FILE_ROLL");
    agentProps.put("agent.sinks.roll-01.sink.directory", sinkOutputDir.getPath());
    agentProps.put("agent.sinks.roll-01.sink.rollInterval", "0");

    agentProps.put("agent.sources", Joiner.on(" ").join(spooldirSrcNames));
    agentProps.put("agent.channels", channelName);
    agentProps.put("agent.sinks", "roll-01");
  }

  @After
  public void teardown() throws Exception {
    StagedInstall.getInstance().stopAgent();
  }

  private String getTestString(int dirNum, int fileNum) {
    return String.format("Test dir %03d, test file %03d.\n", dirNum, fileNum);
  }

  /** Create a bunch of test files. */
  private void createInputTestFiles(List<File> spoolDirs, int numFiles, int startNum)
      throws IOException {
    int numSpoolDirs = spoolDirs.size();
    for (int dirNum = 0; dirNum < numSpoolDirs; dirNum++) {
      File spoolDir = spoolDirs.get(dirNum);
      for (int fileNum = startNum; fileNum < numFiles; fileNum++) {
        // Stage the files on what is almost certainly the same FS partition.
        File tmp = new File(spoolDir.getParent(), UUID.randomUUID().toString());
        Files.append(getTestString(dirNum, fileNum), tmp, Charsets.UTF_8);
        File dst = new File(spoolDir, String.format("test-file-%03d", fileNum));
        // Ensure we move them into the spool directory atomically, if possible.
        assertTrue(String.format("Failed to rename %s to %s", tmp, dst),
            tmp.renameTo(dst));
      }
    }
  }

  private void validateSeenEvents(File outDir, int outFiles, int dirs, int events)
      throws IOException {
    File[] sinkOutputDirChildren = outDir.listFiles();
    assertEquals("Unexpected number of files in output dir",
        outFiles, sinkOutputDirChildren.length);
    Set<String> seenEvents = Sets.newHashSet();
    for (File outFile : sinkOutputDirChildren) {
      List<String> lines = Files.readLines(outFile, Charsets.UTF_8);
      for (String line : lines) {
        seenEvents.add(line);
      }
    }
    for (int dirNum = 0; dirNum < dirs; dirNum++) {
      for (int fileNum = 0; fileNum < events; fileNum++) {
        String event = getTestString(dirNum, fileNum).trim();
        assertTrue("Missing event: {" + event + "}", seenEvents.contains(event));
      }
    }
  }

  @Test
  public void testManySpooldirs() throws Exception {
    LOGGER.debug("testManySpooldirs() started.");

    StagedInstall.getInstance().startAgent("agent", agentProps);

    final int NUM_FILES_PER_DIR = 10;
    createInputTestFiles(spoolDirs, NUM_FILES_PER_DIR, 0);

    TimeUnit.SECONDS.sleep(10); // Wait for sources and sink to process files

    // Ensure we received all events.
    validateSeenEvents(sinkOutputDir,1, spoolDirs.size(), NUM_FILES_PER_DIR);
    LOGGER.debug("Processed all the events!");

    LOGGER.debug("testManySpooldirs() ended.");
  }

}
