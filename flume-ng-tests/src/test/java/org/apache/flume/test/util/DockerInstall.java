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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Attempts to setup a staged install using explicitly specified tar-ball
 * distribution or by using relative path into the flume-ng-dist module.
 */
public class DockerInstall extends StagedInstall {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerInstall.class);

  private String configFilePath;

  private ProcessShutdownHook shutdownHook;
  private ProcessInputStreamConsumer consumer;
  private String containerId;

  private static DockerInstall INSTANCE;
  private String dockerImageId;

  public static synchronized DockerInstall getInstance() throws Exception {
    if (INSTANCE == null) {
      INSTANCE = new DockerInstall();
    }
    return INSTANCE;
  }

  public synchronized boolean isRunning() {
    return containerId != null;
  }

  public synchronized void stopAgent() throws Exception {
    if (containerId == null) {
      throw new Exception("Process not found");
    }

    LOGGER.info("Shutting down agent process");

    ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
    builder.add("/bin/sh");
    builder.add("-c");
    builder.add("'docker kill " +  containerId + "'");

    List<String> cmdArgs = builder.build();

    File tempShellFileKiller = File.createTempFile("docker", ".sh");
    tempShellFileKiller.setExecutable(true);
    tempShellFileKiller.deleteOnExit();
    Files.write(Joiner.on(" ").join(cmdArgs).getBytes(StandardCharsets.UTF_8), tempShellFileKiller);

    ProcessBuilder processKiller = new ProcessBuilder(tempShellFileKiller.getAbsolutePath());

    Process killer = processKiller.start();
    killer.waitFor();

    containerId = null;
    consumer.interrupt();
    consumer = null;
    configFilePath = null;
    Runtime.getRuntime().removeShutdownHook(shutdownHook);
    shutdownHook = null;

    Thread.sleep(3000); // sleep for 3s to let system shutdown
  }

  public synchronized void startAgent(String name, Properties properties) throws Exception {
    startAgent(name, properties, new HashMap<>(), new HashMap<>(), new ArrayList<>());
  }

  public synchronized void startAgent(String name, Properties properties, List<File> mountPoints) throws Exception {
    startAgent(name, properties, new HashMap<>(), new HashMap<>(), mountPoints);
  }

  public synchronized void startAgent(
      String name, Properties properties,  Map<String, String> environmentVariables,
      Map<String, String> commandOptions, List<File> mountPoints)
      throws Exception {
    Preconditions.checkArgument(!name.isEmpty(), "agent name must not be empty");
    Preconditions.checkNotNull(properties, "properties object must not be null");

    // State per invocation - config file, process, shutdown hook
    String agentName = name;

    if (containerId != null) {
      throw new Exception("A process is already running");
    }
    LOGGER.info("Starting process for agent: " + agentName + " using config: " + properties);

    File configFile = createConfigurationFile(agentName, properties);
    configFilePath = configFile.getCanonicalPath();

    String configFileName = configFile.getName();
    String logFileName = "flume-" + agentName + "-"
        + configFileName.substring(0, configFileName.indexOf('.')) + ".log";

    LOGGER.info("Created configuration file: " + configFilePath);

    ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
    builder.add("/bin/sh");
    builder.add("-c");


    StringBuilder sb = new StringBuilder("'");
    sb.append("docker run ");
    sb.append(" --detach");
    sb.append(" -v ").append(confDirPath).append(":").append(confDirPath);
    sb.append(" -v ").append(getStageDir().toString()).append(":").append(getStageDir().toString());
    sb.append(" -v ").append(logDirPath).append(":").append(logDirPath);

    mountPoints.forEach(file -> sb.append(" -v ").append(file.toString()).append(":").append(file.toString()));

    sb.append(" -t ").append(dockerImageId).append(" agent");
    sb.append(" --conf ").append(confDirPath);
    sb.append(" --conf-file ").append(configFilePath);
    sb.append(" --name ").append(agentName);
    sb.append(" -D").append(ENV_FLUME_LOG_DIR).append( "=").append(logDirPath);
    sb.append(" -D" ).append( ENV_FLUME_ROOT_LOGGER ).append( "=" ).append( ENV_FLUME_ROOT_LOGGER_VALUE);
    sb.append(" -D" ).append( ENV_FLUME_LOG_FILE ).append( "=").append(logFileName);
    sb.append("'");

    builder.add(sb.toString());

    commandOptions.forEach((key, value) -> builder.add(key, value));

    List<String> cmdArgs = builder.build();

    File tempShellFile = File.createTempFile("docker", ".sh");
    tempShellFile.setExecutable(true);
    tempShellFile.deleteOnExit();
    Files.write(Joiner.on(" ").join(cmdArgs).getBytes(StandardCharsets.UTF_8), tempShellFile);

    ProcessBuilder pb = new ProcessBuilder(tempShellFile.getAbsolutePath());

    Map<String, String> env = pb.environment();
    env.putAll(environmentVariables);

    pb.directory(baseDir);

    Process process = pb.start();

    BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
    StringBuilder containerIdSb = new StringBuilder();
    String line;
    while ( (line = reader.readLine()) != null) {
      containerIdSb.append(line);
     }

    containerId = containerIdSb.toString();

    if (process.exitValue() != 0) {
      throw new RuntimeException("Docker container did not start: " + process.exitValue() + " " + containerId);
    }


    ImmutableList.Builder<String> logBuilder = new ImmutableList.Builder<String>();
    logBuilder.add("/bin/sh");
    logBuilder.add("-c");
    logBuilder.add("'docker logs --follow " +  containerId + "'");

    List<String> logCmdArgs = logBuilder.build();

    File tempLogShellFile = File.createTempFile("docker", ".sh");
    tempLogShellFile.setExecutable(true);
    tempLogShellFile.deleteOnExit();
    Files.write(Joiner.on(" ").join(logCmdArgs).getBytes(StandardCharsets.UTF_8), tempLogShellFile);


    ProcessBuilder logReaderPb = new ProcessBuilder(tempLogShellFile.getAbsolutePath());
    Process logReaderProc = logReaderPb.start();

    consumer = new ProcessInputStreamConsumer(logReaderProc.getInputStream());
    consumer.start();

    shutdownHook = new ProcessShutdownHook();
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    Thread.sleep(3000); // sleep for 3s to let system initialize
  }



  private DockerInstall() throws Exception {

    super();
    dockerImageId = getDockerImageId();
  }


  private static String getDockerImageId() throws Exception {
    File dockerImageIdFile = new File("../flume-ng-dist/target/docker/image-id");
    return Files.readFirstLine(dockerImageIdFile, StandardCharsets.UTF_8);
  }

}
