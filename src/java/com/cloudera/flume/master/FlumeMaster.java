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
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.master.flows.FlowConfigManager;
import com.cloudera.flume.master.logical.LogicalConfigurationManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.server.ReportServer;
import com.cloudera.flume.util.FlumeVMInfo;
import com.cloudera.flume.util.SystemInfo;
import com.cloudera.util.NetUtils;
import com.cloudera.util.StatusHttpServer;

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

  static Logger LOG = Logger.getLogger(FlumeMaster.class);

  /** report key -- hostname of this master */
  static final String REPORTKEY_HOSTNAME = "hostname";
  /** report key -- number of nodes reporting to this master */
  static final String REPORTKEY_NODES_REPORTING_COUNT = "nodes_reporting_count";

  MasterAdminServer configServer;
  MasterClientServer controlServer;
  ReportServer reportServer;

  StatusHttpServer http = null;

  final boolean doHttp;

  final CommandManager cmdman;
  final ConfigurationManager specman;
  final StatusManager statman;
  final MasterAckManager ackman;
  
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

    // TODO (jon) semantics have changed slightly -- different translations have
    // thier configurations partitioned now, only the user entered root
    // configurations are saved.
    ConfigurationManager base = new ConfigManager(cfgStore);
    ConfigurationManager flowedFailovers = new FlowConfigManager.FailoverFlowConfigManager(
        base, statman);
    this.specman = new LogicalConfigurationManager(flowedFailovers,
        new ConfigManager(), statman);

    if (FlumeConfiguration.get().getMasterIsDistributed()) {
      this.ackman = new GossipedMasterAckManager(FlumeConfiguration.get());
    } else {
      this.ackman = new MasterAckManager();
    }
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
    ReportManager.get().add(new FlumeVMInfo(this.uniqueMasterName + "."));
    ReportManager.get().add(new SystemInfo(this.uniqueMasterName + "."));

    if (doHttp) {
      String webPath = FlumeNode.getWebPath(cfg);
      this.http = new StatusHttpServer("flumeconfig", webPath, "0.0.0.0", cfg
          .getMasterHttpPort(), false);
      http.start();
    }

    controlServer = new MasterClientServer(this, FlumeConfiguration.get());
    configServer = new MasterAdminServer(this, FlumeConfiguration.get());
    reportServer = new ReportServer(FlumeConfiguration.get()
        .getReportServerPort());

    ReportManager.get().add(this);
    try {
      controlServer.serve();
      configServer.serve();
      reportServer.serve();
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

      if (reportServer != null) {
        reportServer.stop();
        reportServer = null;
      }

      specman.stop();

      reaper.interrupt();

      FlumeConfiguration cfg = FlumeConfiguration.get();
      if (cfg.getMasterStore().equals(ZK_CFG_STORE)) {
        ZooKeeperService.get().shutdown();
      }
      
    } catch (IOException e) {
      LOG.error("Exception when shutting down master! " + e.getMessage());
      LOG.debug(e, e);
    } catch (Exception e) {
      LOG.error("Exception when shutting down master! " + e.getMessage());
      LOG.debug(e, e);
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
    statman.getReport().toHtml(o);
    specman.getReport().toHtml(o);
    cmdman.getReport().toHtml(o);
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
  public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());

    rpt.setStringMetric(REPORTKEY_HOSTNAME, NetUtils.localhost());

    rpt.setLongMetric(REPORTKEY_NODES_REPORTING_COUNT, this.getKnownNodes()
        .size());

    return rpt;
  }

  /**
   * This is the method that gets run when bin/flume master is executed.
   */
  public static void main(String[] argv) {
    FlumeNode.logVersion(LOG, Level.INFO);
    FlumeNode.logEnvironment(LOG, Level.INFO);

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
      System.exit(0);
    }

    String nodeconfig = FlumeConfiguration.get().getMasterSavefile();

    if (cmd != null && cmd.hasOption("c")) {
      nodeconfig = cmd.getOptionValue("c");
    }

    if (cmd != null && cmd.hasOption("i")) {
      String sid = cmd.getOptionValue("i");
      LOG.info("Setting serverid from command line to be " + sid);
      try {
        int serverid = Integer.parseInt(cmd.getOptionValue("i"));
        FlumeConfiguration.get().setInt(FlumeConfiguration.MASTER_SERVER_ID,
            serverid);
      } catch (NumberFormatException e) {
        LOG.error("Couldn't parse server id as integer: " + sid);
        System.exit(0);
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
}
