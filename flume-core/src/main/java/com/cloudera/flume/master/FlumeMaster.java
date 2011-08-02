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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.master.flows.FlowConfigManager;
import com.cloudera.flume.master.logical.LogicalConfigurationManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.server.AvroReportServer;
import com.cloudera.flume.reporter.server.ThriftReportServer;
import com.cloudera.flume.util.FlumeVMInfo;
import com.cloudera.flume.util.SystemInfo;
import com.cloudera.util.CheckJavaVersion;
import com.cloudera.util.InternalHttpServer;
import com.cloudera.util.NetUtils;

/**
 * This is a first cut at a server for distributing configurations to different
 * flume client machines. Right now this is a SPOF and not reliable or
 * persistent, but could eventually hooked into BDB or ZK or something to make
 * configurations more reliable.
 */
public class FlumeMaster implements Reportable {
  protected static final String ZK_CFG_STORE = "zookeeper";
  protected static final String MEMORY_CFG_STORE = "memory";

  protected final FlumeConfiguration cfg;

  static final Logger LOG = LoggerFactory.getLogger(FlumeMaster.class);

  /** report key -- hostname of this master */
  static final String REPORTKEY_HOSTNAME = "hostname";
  /** report key -- number of nodes reporting to this master */
  static final String REPORTKEY_NODES_REPORTING_COUNT = "nodes_reporting_count";

  MasterAdminServer configServer;
  MasterClientServer controlServer;
  /*
   * We create instances of both AvroReportServer and ThriftReportServer, and
   * start the one defined by the flag flume.report.server.rpc.type in the
   * configuration file.
   */
  ThriftReportServer thriftReportServer = null;
  AvroReportServer avroReportServer = null;
  InternalHttpServer http = null;

  final boolean doHttp;

  final CommandManager cmdman;
  final ConfigurationManager specman;
  final StatusManager statman;
  final MasterAckManager ackman;

  final SystemInfo sysInfo = new SystemInfo(this.uniqueMasterName + ".");
  final FlumeVMInfo vmInfo = new FlumeVMInfo(this.uniqueMasterName + ".");

  final String uniqueMasterName;

  Thread reaper;

  // This is a static instance for commands and for the web interface to get to.
  static FlumeMaster instance;

  /**
   * Warning - do not use this constructor if you think it has been called
   * anywhere else! This is also not thread safe.
   * 
   * TODO(henry): Proper singleton implementation
   * 
   * TODO (jon): make doHttp a FlumeConfiguraiton option
   */

  public FlumeMaster() {
    this(FlumeConfiguration.get(), true);
  }

  /**
   * Constructs a FlumeMaster using the default FlumeConfiguration, with the web
   * server off
   */
  public FlumeMaster(FlumeConfiguration cfg) {
    this(cfg, false);
  }

  /**
   * Warning - do not use this constructor if you think it has been called
   * anywhere else! This is also not thread safe.
   * 
   * TODO(henry): Proper singleton implementation
   */
  public FlumeMaster(FlumeConfiguration cfg, boolean doHttp) {
    this.cfg = cfg;
    instance = this;

    this.uniqueMasterName = "flume-master-" + cfg.getMasterServerId();

    this.doHttp = doHttp;
    this.cmdman = new CommandManager();
    ConfigStore cfgStore = createConfigStore(FlumeConfiguration.get());
    this.statman = new StatusManager();

    // configuration manager translate user entered configs

    if (FlumeConfiguration.get().getMasterIsDistributed()) {
      LOG.info("Distributed master, disabling all config translations");
      ConfigurationManager base = new ConfigManager(cfgStore);
      this.specman = base;
    } else {
      // TODO (jon) translated configurations cause problems in multi-master
      // situations. For now we disallow translation.
      LOG.info("Single master, config translations enabled");
      ConfigurationManager base = new ConfigManager(cfgStore);
      ConfigurationManager flowedFailovers = new FlowConfigManager.FailoverFlowConfigManager(
          base, statman);
      this.specman = new LogicalConfigurationManager(flowedFailovers,
          new ConfigManager(), statman);
    }

    if (FlumeConfiguration.get().getMasterIsDistributed()) {
      this.ackman = new GossipedMasterAckManager(FlumeConfiguration.get());
    } else {
      this.ackman = new MasterAckManager();
    }
  }

  /**
   * Completely generic and pluggable Flume master constructor. Used for test
   * cases. Webserver is by default on.
   */
  public FlumeMaster(CommandManager cmd, ConfigurationManager cfgMan,
      StatusManager stat, MasterAckManager ack, FlumeConfiguration cfg,
      boolean doHttp) {
    instance = this;
    this.doHttp = doHttp;
    this.cmdman = cmd;
    this.specman = cfgMan;
    this.statman = stat;
    this.ackman = ack;
    this.cfg = cfg;
    this.uniqueMasterName = "flume-master-" + cfg.getMasterServerId();
  }

  /**
   * Completely generic and pluggable Flume master constructor. Used for test
   * cases. Webserver is by default off.
   */
  public FlumeMaster(CommandManager cmd, ConfigurationManager cfgMan,
      StatusManager stat, MasterAckManager ack, FlumeConfiguration cfg) {
    instance = this;
    this.doHttp = false;
    this.cmdman = cmd;
    this.specman = cfgMan;
    this.statman = stat;
    this.ackman = ack;
    this.cfg = cfg;
    this.uniqueMasterName = "flume-master-" + cfg.getMasterServerId();
  }

  /**
   * This hook makes it easy for web apps and jsps to get the current FlumeNode
   * instance. This is used to test the FlumeNode related jsps.
   */
  synchronized public static FlumeMaster getInstance() {
    if (instance == null) {
      instance = new FlumeMaster();
    }
    return instance;
  }

  /**
   * Helper function to parse the configuration to decide which kind of config
   * store to start
   */
  public static ConfigStore createConfigStore(FlumeConfiguration cfg) {
    ConfigStore cfgStore;
    if (cfg.getMasterStore().equals(ZK_CFG_STORE)) {
      cfgStore = new ZooKeeperConfigStore();
    } else if (cfg.getMasterStore().equals(MEMORY_CFG_STORE)) {
      if (cfg.getMasterIsDistributed()) {
        throw new IllegalStateException("Can't use non-zookeeper store with "
            + "distributed Master");
      }
      cfgStore = new MemoryBackedConfigStore();
    } else {
      throw new IllegalArgumentException("Unsupported config store: "
          + cfg.getMasterStore());
    }
    return cfgStore;
  }

  /**
   * Returns a cmd id number that can be used to check status of the command.
   */
  public long submit(Command cmd) {
    return cmdman.submit(cmd);
  }

  public void serve() throws IOException {
    if (cfg.getMasterStore().equals(ZK_CFG_STORE)) {
      try {
        ZooKeeperService.getAndInit(cfg);
      } catch (InterruptedException e) {
        throw new IOException("Unexpected interrupt when starting ZooKeeper", e);
      }
    }
    ReportManager.get().add(vmInfo);
    ReportManager.get().add(sysInfo);

    if (doHttp) {
      String webPath = FlumeNode.getWebPath(cfg);

      http = new InternalHttpServer();

      /*
      this.http = new StatusHttpServer("flumeconfig", webPath, "0.0.0.0", cfg
          .getMasterHttpPort(), false);
      http.addServlet(jerseyMasterServlet(), "/master/*");
      */
      http.setBindAddress("0.0.0.0");
      http.setPort(cfg.getMasterHttpPort());
      http.setWebappDir(new File(FlumeConfiguration.get().getMasterWebappRoot()));
      http.start();
    }

    controlServer = new MasterClientServer(this, FlumeConfiguration.get());
    configServer = new MasterAdminServer(this, FlumeConfiguration.get());
    /*
     * We instantiate both kinds of report servers below, but no resources are
     * allocated till we call serve() on them.
     */
    avroReportServer = new AvroReportServer(FlumeConfiguration.get()
        .getReportServerPort());
    thriftReportServer = new ThriftReportServer(FlumeConfiguration.get()
        .getReportServerPort());

    ReportManager.get().add(this);
    try {
      controlServer.serve();
      configServer.serve();
      /*
       * Start the Avro/Thrift ReportServer based on the flag set in the
       * configuration file.
       */
      if (cfg.getReportServerRPC() == cfg.RPC_TYPE_AVRO) {
        avroReportServer.serve();
      } else {
        thriftReportServer.serve();
      }
    } catch (TTransportException e1) {
      throw new IOException("Error starting control or config server", e1);
    }
    cmdman.start();
    ackman.start();
    specman.start();

    // TODO (jon) clean shutdown
    reaper = new Thread("Lost node reaper") {
      @Override
      public void run() {
        try {
          while (true) {
            Thread.sleep(FlumeConfiguration.get().getConfigHeartbeatPeriod());
            statman.checkup();
          }
        } catch (InterruptedException e) {
          LOG.error("Reaper thread unexpectedly interrupted:" + e.getMessage());
          LOG.debug("Lost node reaper unexpectedly interrupted", e);
        }

      }
    };

    reaper.start();
  }

  /**
   * Shutdown all the various servers.
   */
  public void shutdown() {
    try {
      if (http != null) {
        try {
          http.stop();
        } catch (Exception e) {
          LOG.error("Error stopping FlumeMaster", e);
        }
        http = null;
      }

      cmdman.stop();

      ackman.stop();

      if (configServer != null) {
        configServer.stop();
        configServer = null;
      }

      if (controlServer != null) {
        controlServer.stop();
        controlServer = null;
      }
      /*
       * Close the reportserver which started.
       */
      if (cfg.getReportServerRPC() == cfg.RPC_TYPE_AVRO) {
        if (avroReportServer != null) {
          avroReportServer.stop();
          avroReportServer = null;
        }
      } else {
        if (thriftReportServer != null) {
          thriftReportServer.stop();
          thriftReportServer = null;
        }
      }
      specman.stop();

      reaper.interrupt();

      FlumeConfiguration cfg = FlumeConfiguration.get();
      if (cfg.getMasterStore().equals(ZK_CFG_STORE)) {
        ZooKeeperService.get().shutdown();
      }

    } catch (Exception e) {
      LOG.error("Exception when shutting down master! " + e.getMessage());
      LOG.debug(e.getMessage(), e);
    }

  }

  /**
   * Used by internal web app for generating web page with master status
   * information.
   */
  public String reportHtml() {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      OutputStreamWriter w = new OutputStreamWriter(baos);
      reportHtml(w);
      w.flush();
      return baos.toString();
    } catch (IOException e) {
      LOG.error("html report generation failed", e);
    }
    return "";
  }

  /**
   * Generates html 1.0 data that displays the configuration status of the flume
   * system.
   */

  public void reportHtml(Writer o) throws IOException {
    statman.getMetrics().toHtml(o);
    specman.getMetrics().toHtml(o);
    cmdman.getMetrics().toHtml(o);
  }

  /**
   * Return a list of the names of any nodes that have been seen. Used by the
   * web interface to populate choice inputs.
   */
  public Set<String> getKnownNodes() {
    return statman.getNodeStatuses().keySet();
  }

  public ConfigurationManager getSpecMan() {
    return specman;
  }

  public StatusManager getStatMan() {
    return statman;
  }

  public MasterAckManager getAckMan() {
    return ackman;
  }

  public CommandManager getCmdMan() {
    return cmdman;
  }

  @Override
  public String getName() {
    return this.uniqueMasterName;
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());

    rpt.setStringMetric(REPORTKEY_HOSTNAME, NetUtils.localhost());

    rpt.setLongMetric(REPORTKEY_NODES_REPORTING_COUNT, this.getKnownNodes()
        .size());

    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

  /**
   * This returns true if the host running this process is in the list of master
   * servers. The index is set in the FlumeConfiguration. If the host doesn't
   * match, false is returned. If the hostnames in the master server list fail
   * to resolve, an exception is thrown.
   */

  public static boolean inferMasterHostID() throws UnknownHostException,
      SocketException {
    String masters = FlumeConfiguration.get().getMasterServers();
    String[] mtrs = masters.split(",");

    int idx = NetUtils.findHostIndex(mtrs);
    if (idx < 0) {

      String localhost = NetUtils.localhost();
      LOG.error("Attempted to start a master '{}' that is not "
          + "in the master servers list: '{}'", localhost, mtrs);
      // localhost ips weren't in the list.
      return false;
    }

    FlumeConfiguration.get().setInt(FlumeConfiguration.MASTER_SERVER_ID, idx);
    LOG.info("Inferred master server index {}", idx);
    return true;

  }

  /**
   * This is the method that gets run when bin/flume master is executed.
   */
  public static void main(String[] argv) {
    FlumeNode.logVersion(LOG);
    FlumeNode.logEnvironment(LOG);
    // Make sure the Java version is not older than 1.6
    if (!CheckJavaVersion.isVersionOk()) {
      LOG
          .error("Exiting because of an old Java version or Java version in bad format");
      System.exit(-1);
    }
    FlumeConfiguration.hardExitLoadConfig(); // if config file is bad hardexit.

    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("c", true, "Load config from file");
    options.addOption("f", false, "Use fresh (empty) flume configs");
    options.addOption("i", true, "Server id (an integer from 0 up)");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeNode", options, true);
      System.exit(1);
    }
    
    FormatFactory.loadOutputFormatPlugins();

    String nodeconfig = FlumeConfiguration.get().getMasterSavefile();

    if (cmd != null && cmd.hasOption("c")) {
      nodeconfig = cmd.getOptionValue("c");
    }

    if (cmd != null && cmd.hasOption("i")) {
      // if manually overridden by command line, accept it, live with
      // consequences.
      String sid = cmd.getOptionValue("i");
      LOG.info("Setting serverid from command line to be " + sid);
      try {
        int serverid = Integer.parseInt(cmd.getOptionValue("i"));
        FlumeConfiguration.get().setInt(FlumeConfiguration.MASTER_SERVER_ID,
            serverid);
      } catch (NumberFormatException e) {
        LOG.error("Couldn't parse server id as integer: " + sid);
        System.exit(1);
      }
    } else {
      // attempt to auto detect master id.
      try {
        if (!inferMasterHostID()) {
          System.exit(1);
        }
      } catch (Exception e) {
        // master needs to be valid to continue;
        LOG.error("Unable to resolve host '{}' ", e.getMessage());
        System.exit(1);
      }

    }

    // This will instantiate and read FlumeConfiguration - so make sure that
    // this is *after* we set the MASTER_SERVER_ID above.
    FlumeMaster config = new FlumeMaster();
    LOG.info("Starting flume master on: " + NetUtils.localhost());
    LOG.info(" Working Directory is: " + new File(".").getAbsolutePath());

    try {
      boolean autoload = FlumeConfiguration.get().getMasterSavefileAutoload();
      try {
        if (autoload && (cmd == null || (cmd != null && !cmd.hasOption("f")))) {
          // autoload a config?
          config.getSpecMan().loadConfigFile(nodeconfig);
        }
      } catch (IOException e) {
        LOG.warn("Could not autoload config from " + nodeconfig + " because "
            + e.getMessage());
      }
      config.serve();

    } catch (IOException e) {
      LOG.error("IO problem: " + e.getMessage());
      LOG.debug("IOException", e);
    }
  }

  public FlumeVMInfo getVMInfo() {
    return vmInfo;
  }

  public SystemInfo getSystemInfo() {
    return sysInfo;
  }
}
