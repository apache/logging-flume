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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


/**
 * Attempts to setup a staged install using explicitly specified tar-ball
 * distribution or by using relative path into the flume-ng-dist module.
 */
public class StagedInstall {

  private static final Logger LOGGER = Logger.getLogger(StagedInstall.class);

  public static final String PROP_PATH_TO_DIST_TARBALL =
      "flume.dist.tarball";

  public static final String ENV_FLUME_LOG_DIR = "flume.log.dir";
  public static final String ENV_FLUME_ROOT_LOGGER = "flume.root.logger";
  public static final String ENV_FLUME_ROOT_LOGGER_VALUE = "DEBUG,LOGFILE";
  public static final String ENV_FLUME_LOG_FILE = "flume.log.file";

  private final File stageDir;
  private final File baseDir;
  private final String launchScriptPath;
  private final String confDirPath;
  private final String logDirPath;

  // State per invocation - config file, process, shutdown hook
  private String agentName;
  private String configFilePath;
  private Process process;
  private ProcessShutdownHook shutdownHook;
  private ProcessInputStreamConsumer consumer;

  private static StagedInstall INSTANCE;

  public synchronized static StagedInstall getInstance() throws Exception {
    if (INSTANCE == null) {
      INSTANCE = new StagedInstall();
    }
    return INSTANCE;
  }

  public synchronized boolean isRunning() {
    return process != null;
  }

  public synchronized void stopAgent() throws Exception {
    if (process == null) {
      throw new Exception("Process not found");
    }

    LOGGER.info("Shutting down agent process");
    process.destroy();
    process = null;
    consumer.interrupt();
    consumer = null;
    configFilePath = null;
    Runtime.getRuntime().removeShutdownHook(shutdownHook);
    shutdownHook = null;

    Thread.sleep(3000); // sleep for 3s to let system shutdown
  }

  public synchronized void startAgent(String name, String configResource)
      throws Exception {
    if (process != null) {
      throw new Exception("A process is already running");
    }

    Properties props = new Properties();
    props.load(ClassLoader.getSystemResourceAsStream(configResource));

    startAgent(name, props);
  }

  public synchronized void startAgent(String name, Properties properties)
      throws Exception {
    Preconditions.checkArgument(!name.isEmpty(), "agent name must not be empty");
    Preconditions.checkNotNull(properties, "properties object must not be null");

    agentName = name;

    if (process != null) {
      throw new Exception("A process is already running");
    }
    LOGGER.info("Starting process for agent: " + agentName + " using config: "
       + properties);

    File configFile = createConfigurationFile(agentName, properties);
    configFilePath = configFile.getCanonicalPath();

    String configFileName = configFile.getName();
    String logFileName = "flume-" + agentName + "-"
        + configFileName.substring(0, configFileName.indexOf('.')) + ".log";

    LOGGER.info("Created configuration file: " + configFilePath);

    ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>();
    builder.add(launchScriptPath);
    builder.add("agent");
    builder.add("--conf", confDirPath);
    builder.add("--conf-file", configFilePath);
    builder.add("--name", agentName);
    builder.add("-D" + ENV_FLUME_LOG_DIR + "=" + logDirPath);
    builder.add("-D" + ENV_FLUME_ROOT_LOGGER + "="
            + ENV_FLUME_ROOT_LOGGER_VALUE);
    builder.add("-D" + ENV_FLUME_LOG_FILE + "=" + logFileName);

    List<String> cmdArgs = builder.build();

    LOGGER.info("Using command: " + Joiner.on(" ").join(cmdArgs));

    ProcessBuilder pb = new ProcessBuilder(cmdArgs);

    Map<String, String> env = pb.environment();

    LOGGER.debug("process environment: " + env);
    pb.directory(baseDir);
    pb.redirectErrorStream(true);

    process = pb.start();
    consumer = new ProcessInputStreamConsumer(process.getInputStream());
    consumer.start();

    shutdownHook = new ProcessShutdownHook();
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    Thread.sleep(3000); // sleep for 3s to let system initialize
  }

  public synchronized void reconfigure(Properties properties) throws Exception {
    File configFile = createConfigurationFile(agentName, properties);
    Files.copy(configFile, new File(configFilePath));
    configFile.delete();
    LOGGER.info("Updated agent config file: " + configFilePath);
  }

  public synchronized File getStageDir() {
    return stageDir;
  }

  private File createConfigurationFile(String agentName, Properties properties)
      throws Exception {
    Preconditions.checkNotNull(properties, "properties object must not be null");

    File file = File.createTempFile("agent", "config.properties", stageDir);

    OutputStream os = null;
    try {
      os = new FileOutputStream(file);
      properties.store(os, "Config file for agent: " + agentName);
    } catch (Exception ex) {
      LOGGER.error("Failed to create config file: " + file, ex);
      throw ex;
    } finally {
      if (os != null) {
        try {
          os.close();
        } catch (Exception ex) {
          LOGGER.warn("Unable to close config file stream", ex);
        }
      }
    }

    return file;
  }

  private StagedInstall() throws Exception {

    String tarballPath = System.getProperty(PROP_PATH_TO_DIST_TARBALL);
    if (tarballPath == null || tarballPath.trim().length() == 0) {
      LOGGER.info("No value specified for system property: "
              + PROP_PATH_TO_DIST_TARBALL
              + ". Will attempt to use relative path to locate dist tarball.");

      tarballPath = getRelativeTarballPath();
    }

    if (tarballPath == null || tarballPath.trim().length() == 0) {
      throw new Exception("Failed to locate tar-ball distribution. "
          + "Please specify explicitly via system property: "
          + PROP_PATH_TO_DIST_TARBALL);
    }

    // Validate
    File tarballFile = new File(tarballPath);
    if (!tarballFile.isFile() || !tarballFile.canRead()) {
      throw new Exception("The tarball distribution file is invalid: "
          + tarballPath + ". You can override this by explicitly setting the "
          + "system property: " + PROP_PATH_TO_DIST_TARBALL);
    }

    LOGGER.info("Dist tarball to use: " + tarballPath);

    // Now set up a staging directory for this distribution
    stageDir = getStagingDirectory();

    // Deflate the gzip compressed archive
    File tarFile = gunzipDistTarball(tarballFile, stageDir);

    // Untar the deflated file
    untarTarFile(tarFile, stageDir);

    // Delete the tarfile
    tarFile.delete();

    LOGGER.info("Dist tarball staged to: " + stageDir);

    File rootDir = stageDir;
    File[] listBaseDirs = stageDir.listFiles();
    if (listBaseDirs != null && listBaseDirs.length == 1
        && listBaseDirs[0].isDirectory()) {
      rootDir =listBaseDirs[0];
    }
    baseDir = rootDir;

    // Give execute permissions to the bin/flume-ng script
    File launchScript = new File(baseDir, "bin/flume-ng");
    giveExecutePermissions(launchScript);

    launchScriptPath = launchScript.getCanonicalPath();

    File confDir = new File(baseDir, "conf");
    confDirPath = confDir.getCanonicalPath();

    File logDir = new File(baseDir, "logs");
    logDir.mkdirs();

    logDirPath = logDir.getCanonicalPath();

    LOGGER.info("Staged install root directory: " + rootDir.getCanonicalPath());
  }

  private void giveExecutePermissions(File file) throws Exception {
    String[] args = {
        "chmod", "+x", file.getCanonicalPath()
    };
    Runtime.getRuntime().exec(args);
    LOGGER.info("Set execute permissions on " + file);
  }

  private void untarTarFile(File tarFile, File destDir) throws Exception {
    TarArchiveInputStream tarInputStream = null;
    try {
      tarInputStream = new TarArchiveInputStream(new FileInputStream(tarFile));
      TarArchiveEntry entry = null;
      while ((entry = tarInputStream.getNextTarEntry()) != null) {
        String name = entry.getName();
        LOGGER.debug("Next file: " + name);
        File destFile = new File(destDir, entry.getName());
        if (entry.isDirectory()) {
          destFile.mkdirs();
          continue;
        }
        File destParent = destFile.getParentFile();
        destParent.mkdirs();
        OutputStream entryOutputStream = null;
        try {
          entryOutputStream = new FileOutputStream(destFile);
          byte[] buffer = new byte[2048];
          int length = 0;
          while ((length = tarInputStream.read(buffer, 0, 2048)) != -1) {
            entryOutputStream.write(buffer, 0, length);
          }
        } catch (Exception ex) {
          LOGGER.error("Exception while expanding tar file", ex);
          throw ex;
        } finally {
          if (entryOutputStream != null) {
            try {
              entryOutputStream.close();
            } catch (Exception ex) {
              LOGGER.warn("Failed to close entry output stream", ex);
            }
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Exception caught while untarring tar file: "
            + tarFile.getAbsolutePath(), ex);
      throw ex;
    } finally {
      if (tarInputStream != null) {
        try {
          tarInputStream.close();
        } catch (Exception ex) {
          LOGGER.warn("Unable to close tar input stream: "
                + tarFile.getCanonicalPath(), ex);
        }
      }
    }

  }

  private File gunzipDistTarball(File tarballFile, File destDir)
      throws Exception {
    File tarFile = null;

    InputStream tarballInputStream = null;
    OutputStream tarFileOutputStream = null;
    try {
      tarballInputStream = new GZIPInputStream(
          new FileInputStream(tarballFile));
      File temp2File = File.createTempFile("flume", "-bin", destDir);
      String temp2FilePath = temp2File.getCanonicalPath();
      temp2File.delete();

      tarFile = new File(temp2FilePath + ".tar");

      LOGGER.info("Tarball being unzipped to: " + tarFile.getCanonicalPath());

      tarFileOutputStream = new FileOutputStream(tarFile);
      int length = 0;
      byte[] buffer = new byte[10240];
      while ((length = tarballInputStream.read(buffer, 0, 10240)) != -1) {
        tarFileOutputStream.write(buffer, 0, length);
      }

    } catch (Exception ex) {
      LOGGER.error("Exception caught while unpacking the tarball", ex);
      throw ex;
    } finally {
      if (tarballInputStream != null) {
        try {
          tarballInputStream.close();
        } catch (Exception ex) {
          LOGGER.warn("Unable to close input stream to tarball", ex);
        }
      }
      if (tarFileOutputStream != null) {
        try {
          tarFileOutputStream.close();
        } catch (Exception ex) {
          LOGGER.warn("Unable to close tarfile output stream", ex);
        }
      }
    }
    return tarFile;
  }

  private File getStagingDirectory() throws Exception {
    File targetDir = new File("target");
    if (!targetDir.exists() || !targetDir.isDirectory()) {
      // Probably operating from command line. Use temp dir as target
      targetDir = new File(System.getProperty("java.io.tmpdir"));
    }
    File testDir = new File(targetDir, "test");
    testDir.mkdirs();

    File tempFile = File.createTempFile("flume", "_stage", testDir);
    String absFileName = tempFile.getCanonicalPath();
    tempFile.delete();


    File stageDir = new File(absFileName + "_dir");

    if (stageDir.exists()) {
      throw new Exception("Stage directory exists: " +
          stageDir.getCanonicalPath());
    }

    stageDir.mkdirs();

    LOGGER.info("Staging Directory: " + stageDir.getCanonicalPath());

    return stageDir;
  }

  private String getRelativeTarballPath() throws Exception {
    String tarballPath = null;
    File dir = new File("..");
    while (dir != null && dir.isDirectory()) {
      File testFile = new File(dir, "flume-ng-dist/target");

      if (testFile.exists() && testFile.isDirectory()) {
        LOGGER.info("Found candidate dir: " + testFile.getCanonicalPath());
        File[] candidateFiles = testFile.listFiles(new FileFilter() {

          @Override
          public boolean accept(File pathname) {
            String name = pathname.getName();
            if (name != null && name.startsWith("apache-flume-")
                && name.endsWith("-bin.tar.gz")) {
              return true;
            }
            return false;
          }});

        // There should be at most one
        if (candidateFiles != null && candidateFiles.length > 0) {
          if (candidateFiles.length == 1) {
            // Found it
            File file = candidateFiles[0];
            if (file.isFile() && file.canRead()) {
              tarballPath = file.getCanonicalPath();
              LOGGER.info("Found file: " + tarballPath);
              break;
            } else {
              LOGGER.warn("Invalid file: " + file.getCanonicalPath());
            }
          } else {
            StringBuilder sb = new StringBuilder("Multiple candate tarballs");
            sb.append(" found in directory ");
            sb.append(testFile.getCanonicalPath()).append(": ");
            boolean first = true;
            for (File file : candidateFiles) {
              if (first) {
                first = false;
                sb.append(" ");
              } else {
                sb.append(", ");
              }
              sb.append(file.getCanonicalPath());
            }
            sb.append(". All these files will be ignored.");
            LOGGER.warn(sb.toString());
          }
        }
      }

      dir = dir.getParentFile();
    }
    return tarballPath;
  }

  public static void waitUntilPortOpens(String host, int port, long timeout)
      throws IOException, InterruptedException{
    long startTime = System.currentTimeMillis();
    Socket socket;
    boolean connected = false;
    //See if port has opened for timeout.
    while(System.currentTimeMillis() - startTime < timeout){
      try{
        socket = new Socket(host, port);
        socket.close();
        connected = true;
        break;
      } catch (IOException e){
        Thread.sleep(2000);
      }
    }
    if(!connected) {
      throw new IOException("Port not opened within specified timeout.");
    }
  }

  private class ProcessShutdownHook extends Thread {
    public void run() {
      synchronized (StagedInstall.this) {
        if (StagedInstall.this.process != null) {
          process.destroy();
        }
      }
    }
  }

  private static class ProcessInputStreamConsumer extends Thread {
    private final InputStream is;

    private ProcessInputStreamConsumer(InputStream is) {
      this.is = is;
      this.setDaemon(true);
    }

    public void run() {
      try {
        byte[] buffer = new byte[1024];
        int length = 0;
        while ((length = is.read(buffer, 0, 1024)) != -1) {
          LOGGER.info("[process-out] " + new String(buffer, 0, length));
        }
      } catch (Exception ex) {
        LOGGER.warn("Error while reading process stream", ex);
      }
    }
  }
}
