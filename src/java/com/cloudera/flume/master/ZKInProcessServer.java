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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.google.common.base.Preconditions;

/**
 * A single-instance, in-process ZooKeeper server. Potentially part of a wider
 * ensemble. Much of this code is taken from ZK itself.
 * 
 * TODO(henry): this should be refactored into two implementations of a class
 * eventually.
 */
public class ZKInProcessServer {

  /**
   * This class exists only to expose the shutdown method
   */
  static class FlumeZKQuorumPeerMain extends QuorumPeerMain {
    public void doShutdown() {
      Preconditions.checkNotNull(this.quorumPeer,
          "QuorumPeer is null in shutdown!");
      this.quorumPeer.shutdown();
    }
  }

  /**
   * This class exists only to expose the shutdown method
   */
  static class FlumeZKServerMain extends ZooKeeperServerMain {
    public void doShutdown() {
      this.shutdown();
    }
  }

  final static Logger LOG = Logger.getLogger(ZKInProcessServer.class);

  FlumeZKQuorumPeerMain quorumPeer = null;
  FlumeZKServerMain zkServerMain = null;
  QuorumPeerConfig config = new QuorumPeerConfig();

  protected boolean standalone = false;

  /**
   * If block is set, wait until the server comes up
   */
  protected void createInstanceFromConfig(boolean block) throws IOException,
      InterruptedException {
    final ServerConfig serverConfig = new ServerConfig();
    if (standalone) {
      zkServerMain = new FlumeZKServerMain();
    } else {
      quorumPeer = new FlumeZKQuorumPeerMain();
    }
    new Thread() {
      public void run() {
        if (standalone) {
          this.setName("ZooKeeper standalone thread");

          LOG.info("Starting ZooKeeper server");
          serverConfig.readFrom(config);
          try {
            zkServerMain.runFromConfig(serverConfig);
          } catch (IOException e) {
            LOG.error("Couldn't start ZooKeeper server!", e);
          }
        } else {
          this.setName("ZooKeeper thread");
          try {
            LOG.info("Starting ZooKeeper server");
            quorumPeer.runFromConfig(config);
          } catch (IOException e) {
            LOG.error("Couldn't start ZooKeeper server!", e);
          }
        }
      }
    }.start();

    if (block
        && !waitForServerUp("0.0.0.0", config.getClientPortAddress().getPort(),
            15000)) {
      throw new IOException(
          "ZooKeeper server did not come up within 15 seconds");
    }
  }

  /**
   * Utility function to send a command to the internal server. ZK servers
   * accepts 4 byte command strings to test their liveness.
   */
  protected String send4LetterWord(String host, int port, String cmd)
      throws IOException {
    Preconditions.checkArgument(quorumPeer != null || zkServerMain != null);
    Preconditions.checkArgument(cmd.length() == 4);
    Socket sock = new Socket(host, port);
    BufferedReader reader = null;
    try {
      OutputStream outstream = sock.getOutputStream();
      outstream.write(cmd.getBytes());
      outstream.flush();

      reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line + "\n");
      }
      return sb.toString();
    } finally {
      sock.close();
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Wait and see if the server has come up yet by polling.
   */
  protected boolean waitForServerUp(String host, int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        String result = send4LetterWord(host, port, "stat");
        if (result.startsWith("Zookeeper version:")) {
          return true;
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("server " + host + ":" + port + " not up yet");
        LOG.debug("Exception message", e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        return false;
      }
    }
    return false;
  }

  /**
   * Wait for server to exit by polling and sleeping periodically.
   */
  protected boolean waitForServerDown(String host, int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        send4LetterWord(host, port, "stat");
      } catch (IOException e) {
        return true;
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  protected void createDirs(String dataDir, String logDir, int myid)
      throws IOException {
    // Create the data directory
    File datadir = new File(dataDir);
    if (datadir.exists()) {
      if (!datadir.isDirectory()) {
        throw new IOException("Datadir " + datadir
            + " exists and is not a directory");
      }
    } else {
      if (!datadir.mkdirs()) {
        throw new IOException("Unable to create datadir " + datadir);
      }
    }

    File logdir = new File(logDir);
    if (logdir.exists()) {
      if (!logdir.isDirectory()) {
        throw new IOException("Logdir " + logdir
            + " exists and is not a directory");
      }
    } else {
      if (!logdir.mkdirs()) {
        throw new IOException("Unable to create logdir " + logdir);
      }
    }

    // Create the id file
    File idfile = new File(dataDir + "/myid");
    if (idfile.exists()) {
      LOG.info("Removing idfile " + idfile);

      if (!idfile.delete()) {
        throw new IOException("Couldn't delete idfile " + idfile);
      }
    }

    // create does not list filename if operations fails due to permissions
    boolean createSuccess = false;
    try {
      createSuccess = idfile.createNewFile();
    } catch (IOException e) {
      throw new IOException("Do not have permissions to create idfile "
          + idfile);
    }
    if (!createSuccess) {
      throw new IOException("Couldn't create idfile " + idfile);
    }

    LOG.info("Creating " + idfile);
    FileWriter fw = new FileWriter(idfile);
    fw.write(Integer.valueOf(myid).toString() + "\n");
    fw.close();
  }

  /**
   * Construct a new standalone server to run on supplied port number
   */
  public ZKInProcessServer(int clientPort, String logdir) throws IOException {
    this.standalone = true;
    Properties properties = new Properties();
    properties
        .setProperty("clientPort", Integer.valueOf(clientPort).toString());
    properties.setProperty("dataDir", logdir + "/server-0");
    properties.setProperty("dataLogDir", logdir + "/logs-0");
    properties.setProperty("tickTime", "2000");
    properties.setProperty("initLimit", "10");
    properties.setProperty("syncLimit", "5");
    properties.setProperty("electionAlg", "3");
    properties.setProperty("maxClientCnxns", "0");
    createDirs(logdir + "/server-0", logdir + "/logs-0", 0);
    try {
      config.parseProperties(properties);
    } catch (ConfigException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a server from a configuration
   */
  public ZKInProcessServer(FlumeConfiguration conf) throws IOException,
      ConfigException {
    // This is about as elegant as a kick in the teeth.
    Properties properties = new Properties();
    properties.setProperty("tickTime", Integer.valueOf(
        conf.getMasterZKTickTime()).toString());
    properties.setProperty("initLimit", Integer.valueOf(
        conf.getMasterZKInitLimit()).toString());
    properties.setProperty("syncLimit", Integer.valueOf(
        conf.getMasterZKInitLimit()).toString());
    properties.setProperty("dataDir", conf.getMasterZKLogDir() + "/server-"
        + conf.getMasterServerId());
    properties.setProperty("clientPort", Integer.valueOf(
        conf.getMasterZKClientPort()).toString());
    properties.setProperty("electionAlg", Integer.valueOf(3).toString());
    properties.setProperty("maxClientCnxns", "0");

    // Now set the server properties
    String[] hosts = conf.getMasterZKServers().split(",");
    int count = 0;
    for (String l : hosts) {
      String[] kv = l.split(":");
      Preconditions.checkState(kv.length == 4);
      // kv[0] is the hostname, kv[2] is the quorumport,
      // kv[3] is the electionport kv[1] is the (unused) client port
      properties.setProperty("server." + count,
          kv[0] + ":" + kv[2] + ":" + kv[3]);
      ++count;
    }
    int serverid = conf.getMasterServerId();
    createDirs(conf.getMasterZKLogDir() + "/server-" + serverid, conf
        .getMasterZKLogDir()
        + "/logs-" + serverid, serverid);
    LOG.info(properties);
    config.parseProperties(properties);
    this.standalone = false;
  }

  /**
   * This constructor typically used for testing
   */
  public ZKInProcessServer(Properties properties) throws IOException,
      ConfigException {
    createDirs(properties.getProperty("dataDir"), properties
        .getProperty("dataLogDir"), Integer.parseInt(properties
        .getProperty("serverID")));
    config.parseProperties(properties);

    this.standalone = false;
  }

  /**
   * Try to start the server, waiting until it responds to inquiries to return.
   */
  synchronized public void start() throws IOException, InterruptedException {
    createInstanceFromConfig(true);
  }

  /**
   * Try to start the server, without waiting until it responds to inquiries to
   * return.
   */
  synchronized public void startWithoutWaiting() throws IOException,
      InterruptedException {
    createInstanceFromConfig(false);
  }

  /**
   * Closes the connection factory.
   */
  synchronized public boolean stop() {
    if (standalone && zkServerMain != null) {
      zkServerMain.doShutdown();
    }
    if (!standalone && quorumPeer != null) {
      quorumPeer.doShutdown();
    }
    boolean ret = waitForServerDown("0.0.0.0", config.getClientPortAddress()
        .getPort(), 5000);
    quorumPeer = null;
    zkServerMain = null;
    return ret;
  }
}
