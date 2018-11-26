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

import org.apache.commons.io.FileUtils;
import org.apache.flume.test.util.StagedInstall;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestConfig {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestConfig.class);

  @ClassRule
  public static final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  private Properties agentProps;
  private Map<String, String> agentEnv;
  private Map<String, String> agentOptions;
  private File sinkOutputDir1;
  private File sinkOutputDir2;
  private File sinkOutputDir3;
  private File hadoopCredStore;

  @Before
  public void setup() throws Exception {

    File agentDir = StagedInstall.getInstance().getStageDir();
    LOGGER.debug("Using agent stage dir: {}", agentDir);

    File testDir = new File(agentDir, TestConfig.class.getName());
    if (testDir.exists()) {
      FileUtils.deleteDirectory(testDir);
    }
    assertTrue(testDir.mkdirs());

    agentProps = new Properties();
    agentEnv = new HashMap<>();
    agentOptions = new HashMap<>();
    agentOptions.put("-C", getAdditionalClassPath());

    // Create the rest of the properties file
    agentProps.put("agent.sources.seq-01.type", "seq");
    agentProps.put("agent.sources.seq-01.totalEvents", "100");
    agentProps.put("agent.sources.seq-01.channels", "mem-01 mem-02 mem-03");
    agentProps.put("agent.channels.mem-01.type", "MEMORY");
    agentProps.put("agent.channels.mem-01.capacity", String.valueOf(100000));
    agentProps.put("agent.channels.mem-02.type", "MEMORY");
    agentProps.put("agent.channels.mem-02.capacity", String.valueOf(100000));
    agentProps.put("agent.channels.mem-03.type", "MEMORY");
    agentProps.put("agent.channels.mem-04.capacity", String.valueOf(100000));

    sinkOutputDir1 = new File(testDir, "out1");
    assertTrue("Unable to create sink output dir: " + sinkOutputDir1.getPath(),
        sinkOutputDir1.mkdir());
    sinkOutputDir2 = new File(testDir, "out2");
    assertTrue("Unable to create sink output dir: " + sinkOutputDir2.getPath(),
        sinkOutputDir2.mkdir());
    sinkOutputDir3 = new File(testDir, "out3");
    assertTrue("Unable to create sink output dir: " + sinkOutputDir3.getPath(),
        sinkOutputDir3.mkdir());

    environmentVariables.set("HADOOP_CREDSTORE_PASSWORD", "envSecret");

    agentEnv.put("dirname_env", sinkOutputDir1.getAbsolutePath());
    agentEnv.put("HADOOP_CREDSTORE_PASSWORD", "envSecret");

    hadoopCredStore = new File(testDir, "credstore.jceks");
    String providerPath = "jceks://file/" + hadoopCredStore.getAbsolutePath();

    ToolRunner.run(
        new Configuration(), new CredentialShell(),
        ("create dirname_hadoop -value " + sinkOutputDir3.getAbsolutePath()
            + " -provider " + providerPath).split(" "));


    agentProps.put("agent.sinks.roll-01.channel", "mem-01");
    agentProps.put("agent.sinks.roll-01.type", "FILE_ROLL");
    agentProps.put("agent.sinks.roll-01.sink.directory", "${filter-01[\"dirname_env\"]}");
    agentProps.put("agent.sinks.roll-01.sink.rollInterval", "0");
    agentProps.put("agent.sinks.roll-02.channel", "mem-02");
    agentProps.put("agent.sinks.roll-02.type", "FILE_ROLL");
    agentProps.put("agent.sinks.roll-02.sink.directory",
        sinkOutputDir2.getParentFile().getAbsolutePath() + "/${filter-02['out2']}");
    agentProps.put("agent.sinks.roll-02.sink.rollInterval", "0");
    agentProps.put("agent.sinks.roll-03.channel", "mem-03");
    agentProps.put("agent.sinks.roll-03.type", "FILE_ROLL");
    agentProps.put("agent.sinks.roll-03.sink.directory", "${filter-03[dirname_hadoop]}");
    agentProps.put("agent.sinks.roll-03.sink.rollInterval", "0");

    agentProps.put("agent.configfilters.filter-01.type", "env");
    agentProps.put("agent.configfilters.filter-02.type", "external");
    agentProps.put("agent.configfilters.filter-02.command", "echo");
    agentProps.put("agent.configfilters.filter-03.type", "hadoop");
    agentProps.put("agent.configfilters.filter-03.credential.provider.path", providerPath);

    agentProps.put("agent.sources", "seq-01");
    agentProps.put("agent.channels", "mem-01 mem-02 mem-03");
    agentProps.put("agent.sinks", "roll-01 roll-02 roll-03");
    agentProps.put("agent.configfilters", "filter-01 filter-02 filter-03");
  }

  private String getAdditionalClassPath() throws Exception {
    URL resource = this.getClass().getClassLoader().getResource("classpath.txt");
    Path path = Paths.get(Objects.requireNonNull(resource).getPath());
    return Files.readAllLines(path).stream().findFirst().orElse("");
  }

  @After
  public void teardown() throws Exception {
    StagedInstall.getInstance().stopAgent();
  }

  private void validateSeenEvents(File outDir, int outFiles, int events)
      throws IOException {
    File[] sinkOutputDirChildren = outDir.listFiles();
    assertEquals("Unexpected number of files in output dir",
        outFiles, sinkOutputDirChildren.length);
    Set<String> seenEvents = new HashSet<>();
    for (File outFile : sinkOutputDirChildren) {
      Scanner scanner = new Scanner(outFile);
      while (scanner.hasNext()) {
        seenEvents.add(scanner.nextLine());
      }
    }
    for (int event = 0; event < events; event++) {
      assertTrue(
          "Missing event: {" + event + "}",
          seenEvents.contains(String.valueOf(event))
      );
    }
  }

  @Test
  public void testConfigReplacement() throws Exception {
    LOGGER.debug("testConfigReplacement() started.");

    StagedInstall.getInstance().startAgent("agent", agentProps, agentEnv, agentOptions);

    TimeUnit.SECONDS.sleep(10); // Wait for sources and sink to process files

    // Ensure we received all events.
    validateSeenEvents(sinkOutputDir1, 1, 100);
    validateSeenEvents(sinkOutputDir2, 1, 100);
    validateSeenEvents(sinkOutputDir3, 1, 100);
    LOGGER.debug("Processed all the events!");

    LOGGER.debug("testConfigReplacement() ended.");
  }

  @Test
  public void testConfigReload() throws Exception {
    LOGGER.debug("testConfigReplacement() started.");

    agentProps.put("agent.channels.mem-01.transactionCapacity", "10");
    agentProps.put("agent.sinks.roll-01.sink.batchSize", "20");
    StagedInstall.getInstance().startAgent("agent", agentProps, agentEnv, agentOptions);

    TimeUnit.SECONDS.sleep(10); // Wait for sources and sink to process files

    // This directory is empty due to misconfiguration
    validateSeenEvents(sinkOutputDir1, 0, 0);

    // These are well configured
    validateSeenEvents(sinkOutputDir2, 1, 100);
    validateSeenEvents(sinkOutputDir3, 1, 100);
    LOGGER.debug("Processed all the events!");

    //repair the config
    agentProps.put("agent.channels.mem-01.transactionCapacity", "20");
    StagedInstall.getInstance().reconfigure(agentProps);

    TimeUnit.SECONDS.sleep(40); // Wait for sources and sink to process files
    // Ensure we received all events.
    validateSeenEvents(sinkOutputDir1, 1, 100);

    LOGGER.debug("testConfigReplacement() ended.");
  }

}
