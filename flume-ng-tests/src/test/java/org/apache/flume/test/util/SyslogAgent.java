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

package org.apache.flume.test.util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Syslog Flume Agent.
 * A Syslog source of some kind is configured and a client is available to write
 * messages to the agent. The Flume agents port is randomly assigned (not in use).
 *
 */
public class SyslogAgent {
  private static final Logger LOGGER = Logger.getLogger(SyslogAgent.class);
  private static final Collection<File> tempResources = new ArrayList<File>();
  private static final int DEFAULT_ATTEMPTS = 20;
  private static final long DEFAULT_TIMEOUT = 500L;

  public enum SyslogSourceType {
    TCP("syslogtcp"),
    MULTIPORTTCP("multiport_syslogtcp");

    private final String syslogSourceType;

    private SyslogSourceType(String syslogSourceType) {
      this.syslogSourceType = syslogSourceType;
    }

    public String toString() {
      return syslogSourceType;
    }
  }

  private Properties agentProps;
  private File sinkOutputDir;
  private String keepFields;

  private int port;
  private String hostname;

  BufferedOutputStream client;

  public SyslogAgent() throws IOException {
    hostname = "localhost";
    setRandomPort();
  }

  public void setRandomPort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    port = s.getLocalPort();
    s.close();
  }

  public void configure(SyslogSourceType sourceType) throws IOException {
    /* Create 3 temp dirs, each used as value within agentProps */
    sinkOutputDir = Files.createTempDir();
    tempResources.add(sinkOutputDir);
    final String sinkOutputDirPath = sinkOutputDir.getCanonicalPath();
    LOGGER.info("Created rolling file sink's output dir: "
        + sinkOutputDirPath);

    /* Build props to pass to flume agent */
    agentProps = new Properties();

    // Active sets
    agentProps.put("a1.channels", "c1");
    agentProps.put("a1.sources", "r1");
    agentProps.put("a1.sinks", "k1");

    // c1
    agentProps.put("a1.channels.c1.type", "memory");
    agentProps.put("a1.channels.c1.capacity", "1000");
    agentProps.put("a1.channels.c1.transactionCapacity", "100");

    // r1
    agentProps.put("a1.sources.r1.channels", "c1");
    agentProps.put("a1.sources.r1.type", sourceType.toString());
    agentProps.put("a1.sources.r1.host", hostname);
    if (sourceType.equals(SyslogSourceType.MULTIPORTTCP)) {
      agentProps.put("a1.sources.r1.ports", Integer.toString(port));
    } else {
      agentProps.put("a1.sources.r1.port", Integer.toString(port));
    }

    // k1
    agentProps.put("a1.sinks.k1.channel", "c1");
    agentProps.put("a1.sinks.k1.sink.directory", sinkOutputDirPath);
    agentProps.put("a1.sinks.k1.type", "FILE_ROLL");
    agentProps.put("a1.sinks.k1.sink.rollInterval", "0");
  }

  // Blocks until flume agent boots up.
  public void start(String keepFields) throws Exception {
    this.keepFields = keepFields;

    // Set properties that should be different per agent start and stop.
    agentProps.put("a1.sources.r1.keepFields", keepFields);

    // Recreate temporary directory.
    sinkOutputDir.mkdir();

    /* Start flume agent */
    StagedInstall.getInstance().startAgent("a1", agentProps);

    LOGGER.info("Started flume agent with syslog source on port " + port);

    // Wait for source, channel, sink to start and create client.
    int numberOfAttempts = 0;
    while (client == null) {
      try {
        client = new BufferedOutputStream(new Socket(hostname, port).getOutputStream());
      } catch (IOException e) {
        if (++numberOfAttempts >= DEFAULT_ATTEMPTS) {
          throw new AssertionError("Could not connect to source after "
              + DEFAULT_ATTEMPTS + " attempts with " + DEFAULT_TIMEOUT + " ms timeout.");
        }

        TimeUnit.MILLISECONDS.sleep(DEFAULT_TIMEOUT);
      }
    }
  }

  public boolean isRunning() throws Exception {
    return StagedInstall.getInstance().isRunning();
  }

  public void stop() throws Exception {
    if (client != null) {
      client.close();
    }
    client = null;

    StagedInstall.getInstance().stopAgent();
    for (File tempResource : tempResources) {
      // Should always be a directory.
      FileUtils.deleteDirectory(tempResource);
    }
  }

  public void runKeepFieldsTest() throws Exception {
    /* Create expected output and log message */
    String logMessage = "<34>1 Oct 11 22:14:15 mymachine su: Test\n";
    String expectedOutput = "su: Test\n";
    if (keepFields.equals("true") || keepFields.equals("all")) {
      expectedOutput = logMessage;
    } else if (!keepFields.equals("false") && !keepFields.equals("none")) {
      if (keepFields.indexOf("hostname") != -1) {
        expectedOutput = "mymachine " + expectedOutput;
      }
      if (keepFields.indexOf("timestamp") != -1) {
        expectedOutput = "Oct 11 22:14:15 " + expectedOutput;
      }
      if (keepFields.indexOf("version") != -1) {
        expectedOutput = "1 " + expectedOutput;
      }
      if (keepFields.indexOf("priority") != -1) {
        expectedOutput = "<34>" + expectedOutput;
      }
    }
    LOGGER.info("Created expected output: " + expectedOutput);

    /* Send test message to agent */
    sendMessage(logMessage);

    /* Wait for output file */
    int numberOfListDirAttempts = 0;
    while (sinkOutputDir.listFiles().length == 0) {
      if (++numberOfListDirAttempts >= DEFAULT_ATTEMPTS) {
        throw new AssertionError("FILE_ROLL sink hasn't written any files after "
            + DEFAULT_ATTEMPTS + " attempts with " + DEFAULT_TIMEOUT + " ms timeout.");
      }

      TimeUnit.MILLISECONDS.sleep(DEFAULT_TIMEOUT);
    }

    // Only 1 file should be in FILE_ROLL sink's dir (rolling is disabled)
    File[] sinkOutputDirChildren = sinkOutputDir.listFiles();
    Assert.assertEquals("Expected FILE_ROLL sink's dir to have only 1 child," +
                        " but found " + sinkOutputDirChildren.length + " children.",
                        1, sinkOutputDirChildren.length);

    /* Wait for output file stats to be as expected. */
    File outputDirChild = sinkOutputDirChildren[0];
    int numberOfStatsAttempts = 0;
    while (outputDirChild.length() != expectedOutput.length()) {
      if (++numberOfStatsAttempts >= DEFAULT_ATTEMPTS) {
        throw new AssertionError("Expected output and FILE_ROLL sink's"
            + " lengths did not match after " + DEFAULT_ATTEMPTS
            + " attempts with " + DEFAULT_TIMEOUT + " ms timeout.");
      }

      TimeUnit.MILLISECONDS.sleep(DEFAULT_TIMEOUT);
    }

    File actualOutput = sinkOutputDirChildren[0];
    if (!Files.toString(actualOutput, Charsets.UTF_8).equals(expectedOutput)) {
      LOGGER.error("Actual output doesn't match expected output.\n");
      LOGGER.debug("Output: " + Files.toString(actualOutput, Charsets.UTF_8));
      throw new AssertionError("FILE_ROLL sink's actual output doesn't " +
          "match expected output.");
    }
  }

  private void sendMessage(String message) throws IOException {
    client.write(message.getBytes());
    client.flush();
  }
}
