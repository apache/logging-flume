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

package com.cloudera.flume.agent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.cloudera.flume.VersionInfo;
import com.cloudera.flume.agent.diskfailover.DiskFailoverManager;
import com.cloudera.flume.agent.diskfailover.NaiveFileFailoverManager;
import com.cloudera.flume.agent.durability.NaiveFileWALManager;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.reporter.MasterReportPusher;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.util.FlumeVMInfo;
import com.cloudera.flume.util.SystemInfo;
import com.cloudera.util.CheckJavaVersion;
import com.cloudera.util.FileUtil;
import com.cloudera.util.NetUtils;
import com.cloudera.util.Pair;
import com.cloudera.util.StatusHttpServer;
import com.google.common.base.Preconditions;

/**
 * This is a configurable flume node.
 * 
 * It has four main parts: 1) a http server that shows its status, 2) A
 * LivenessManager. This encapsulates heartbeat communications with master that
 * indicate the node is alive, and can trigger config updates. 3) a MasterRPC
 * layer. This encapsulates the RPC comms with the master. It can be replaced
 * later with a different master communication mechanism (Avro or ZK). 4) A
 * LogicalNodeManager. There now can be more than one logical node on a single
 * node. This class manages them.
 * 
 * Threads in the status server do not prevent exit. Threads in liveness manager
 * will prevent exit as will any running threads in nodethread manager.
 * 
 */
public class FlumeNode implements Reportable {
  final static Logger LOG = Logger.getLogger(FlumeNode.class.getName());
  final static String PHYSICAL_NODE_REPORT_PREFIX = "pn-";
  static final String R_NUM_LOGICAL_NODES = "Logical nodes";

  // hook for jsp/web display
  private static FlumeNode instance;

  // run mode for this node.
  final boolean startHttp; // should this node start its own http status server
  private StatusHttpServer http = null;

  private FlumeVMInfo vmInfo;
  private SystemInfo sysInfo;
  private final LivenessManager liveMan;
  private MasterRPC rpcMan;
  private LogicalNodeManager nodesMan;

  private final MasterReportPusher reportPusher;

  /**
   * (String) logical node name -> WALManager
   */
  private Map<String, WALManager> walMans = new HashMap<String, WALManager>();

  /**
   * (String) logical node name -> DiskFailoverManager
   */
  private Map<String, DiskFailoverManager> failoverMans = new HashMap<String, DiskFailoverManager>();

  final private CollectorAckListener collectorAck;

  final String physicalNodeName;

  /**
   * A FlumeNode constructor with pluggable xxxManagers. This is used for
   * debugging and test cases. The http server is assumed not to be started, and
   * we are not doing a oneshot.
   */
  public FlumeNode(String name, MasterRPC rpc, LogicalNodeManager nodesMan,
      WALManager walMan, DiskFailoverManager dfMan,
      CollectorAckListener colAck, LivenessManager liveman) {
    this.physicalNodeName = name;
    rpcMan = rpc;
    instance = this;
    this.startHttp = false;
    this.nodesMan = nodesMan;
    this.walMans.put(getPhysicalNodeName(), walMan);
    this.failoverMans.put(getPhysicalNodeName(), dfMan);
    this.collectorAck = colAck;
    this.liveMan = liveman;

    this.vmInfo = new FlumeVMInfo(PHYSICAL_NODE_REPORT_PREFIX
        + this.physicalNodeName + ".");

    this.reportPusher = new MasterReportPusher(FlumeConfiguration.get(),
        ReportManager.get(), rpcMan);
    this.sysInfo = new SystemInfo(PHYSICAL_NODE_REPORT_PREFIX
        + this.physicalNodeName + ".");
  }

  public FlumeNode(FlumeConfiguration conf, String nodeName, MasterRPC rpc,
      boolean startHttp, boolean oneshot) {
    this.physicalNodeName = nodeName;
    rpcMan = rpc;
    instance = this;
    this.startHttp = startHttp;
    this.nodesMan = new LogicalNodeManager(nodeName);

    File defaultDir = new File(conf.getAgentLogsDir(), getPhysicalNodeName());
    WALManager walMan = new NaiveFileWALManager(defaultDir);
    this.walMans.put(getPhysicalNodeName(), walMan);
    this.failoverMans.put(getPhysicalNodeName(), new NaiveFileFailoverManager(
        defaultDir));

    // no need for liveness tracker if a one shot execution.
    this.collectorAck = new CollectorAckListener(rpcMan);
    if (!oneshot) {
      this.liveMan = new LivenessManager(nodesMan, rpcMan,
          new FlumeNodeWALNotifier(this.walMans));
      this.reportPusher = new MasterReportPusher(conf, ReportManager.get(),
          rpcMan);
    } else {
      this.liveMan = null;
      this.reportPusher = null;
    }

    this.vmInfo = new FlumeVMInfo(PHYSICAL_NODE_REPORT_PREFIX
        + this.getPhysicalNodeName() + ".");
    this.sysInfo = new SystemInfo(PHYSICAL_NODE_REPORT_PREFIX
        + this.getPhysicalNodeName() + ".");
  }

  public FlumeNode(MasterRPC rpc, boolean startHttp, boolean oneshot) {
    this(FlumeConfiguration.get(), NetUtils.localhost(), rpc, startHttp,
        oneshot);
  }

  public FlumeNode(String nodename, FlumeConfiguration conf, boolean startHttp,
      boolean oneshot) {
    // Use a failover-enabled master RPC, which randomizes the failover order
    this(conf, nodename, new MultiMasterRPC(conf, true), startHttp, oneshot);
  }

  public FlumeNode(FlumeConfiguration conf) {
    this(NetUtils.localhost(), conf, false /* http server */, false /* oneshot */);
  }

  /**
   * This hook makes it easy for web apps and jsps to get the current FlumeNode
   * instance. This is used to test the FlumeNode related jsps.
   */
  public static FlumeNode getInstance() {
    if (instance == null) {
      instance = new FlumeNode(FlumeConfiguration.get());
    }
    return instance;
  }

  public static String getWebPath(FlumeConfiguration conf) {
    String webPath = conf.getWebAppsPath();
    File f = new File(webPath);
    // absolute paths win, but if is not absolute, prefix with flume home
    if (!f.isAbsolute()) {
      String basepath = System.getenv("FLUME_HOME");
      if (basepath == null) {
        LOG.warn("FLUME_HOME not set, potential for odd behavior!");
      }
      File base = new File(basepath, webPath);
      webPath = base.getAbsolutePath();
    }
    return webPath;
  }

  synchronized public void start() {
    FlumeConfiguration conf = FlumeConfiguration.get();
    ReportManager.get().add(vmInfo);
    ReportManager.get().add(sysInfo);
    ReportManager.get().add(this);

    if (startHttp) {
      try {
        String webPath = getWebPath(conf);

        boolean findport = FlumeConfiguration.get().getNodeAutofindHttpPort();
        this.http = new StatusHttpServer("flumeagent", webPath, "0.0.0.0", conf
            .getNodeStatusPort(), findport);
        http.start();
      } catch (IOException e) {
        LOG.error("Flume node failed: " + e.getMessage(), e);
      } catch (Throwable t) {
        LOG.error("Unexcepted exception/error thrown! " + t.getMessage(), t);
      }
    }

    if (reportPusher != null) {
      reportPusher.start();
    }

    if (liveMan != null) {
      liveMan.start();
    }
  }

  synchronized public void stop() {
    if (this.http != null) {
      try {
        http.stop();
      } catch (Exception e) {
        LOG.error("Exception stopping FlumeNode", e);
      }
    }

    if (reportPusher != null) {
      reportPusher.stop();
    }

    if (liveMan != null) {
      liveMan.stop();
    }
  }

  /**
   * This method is currently called by the JSP to display node information.
   */
  public String report() {
    return getReport().toHtml();
  }

  public WALAckManager getAckChecker() {
    if (liveMan == null)
      return null;
    return liveMan.getAckChecker();
  }

  public AckListener getCollectorAckListener() {
    return collectorAck;
  }

  public static void logVersion(Logger log, Level level) {
    log.log(level, "Flume " + VersionInfo.getVersion());
    log.log(level, " rev " + VersionInfo.getRevision());
    log.log(level, "Compiled  on " + VersionInfo.getDate());
  }

  public static void logEnvironment(Logger log, Level level) {
    Properties props = System.getProperties();
    for (Entry<Object, Object> p : props.entrySet()) {
      log.log(level, "System property " + p.getKey() + "=" + p.getValue());
    }
  }

  /**
   * This function checks the agent logs dir to make sure that the process has
   * the ability to the directory if necesary, that the path if it does exist is
   * a directory, and that it can infact create files inside of the directory.
   * If it fails any of these, it throws an exception.
   * 
   * Finally, it checks to see if the path is in /tmp and warns the user that
   * this may not be the best idea.
   */
  public static void nodeConfigChecksOk() throws IOException {
    // TODO (jon) if we add more checks in here, make the different managers
    // responsible for throwing an Exception on construction instead.

    FlumeConfiguration conf = FlumeConfiguration.get();

    String s = conf.getAgentLogsDir();
    File f = new File(s);

    if (!FileUtil.makeDirs(f)) {
      throw new IOException("Path to Log dir cannot be created: '" + s
          + "'.  Check permissions?");
    }

    if (!f.isDirectory()) {
      throw new IOException("Log dir '" + s
          + "' already exists as a file.  Check log dir path.");
    }

    File f2 = null;
    try {
      f2 = File.createTempFile("initcheck", ".test", f);
    } catch (IOException e) {
      throw new IOException("Failure to write in log directory: '" + s
          + "'.  Check permissions?");
    }
    if (!f2.delete()) {
      throw new IOException("Unable to delete " + f2 + " from log directory "
          + "(but writing succeeded) - something is strange here");
    }

    File tmp = new File("/tmp");
    File cur = f;
    while (cur != null) {
      if (cur.equals(tmp)) {
        LOG
            .warn("Log directory is writing inside of /tmp.  This data may not survive reboot!");
        break;
      }
      cur = cur.getParentFile();
    }
  }

  public static void setup(String[] argv) throws IOException {
    logVersion(LOG, Level.INFO);
    logEnvironment(LOG, Level.INFO);
    // Make sure the Java version is not older than 1.6
    if (CheckJavaVersion.checkVersion()) {
      LOG
          .error("Exitting because of an old Java version or Java version in bad format");
      System.exit(-1);
    }
    LOG.info("Starting flume agent on: " + NetUtils.localhost());
    LOG.info(" Working directory is: " + new File(".").getAbsolutePath());

    FlumeConfiguration.hardExitLoadConfig(); // will exit if conf file is bad.

    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("c", true, "Load initial config from cmdline arg");
    options.addOption("n", true, "Set node name");
    options.addOption("s", false,
        "Do not start local flume status server on node");
    options.addOption("1", false,
        "Make flume node one shot (if closes or errors, exits)");
    options.addOption("m", false,
        "Have flume hard exit if in likey gc thrash situation");
    options.addOption("h", false, "Print help information");
    options.addOption("v", false, "Print version information");
    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeNode", options, true);
      return;
    }

    // dump version info only
    if (cmd != null && cmd.hasOption("v")) {
      return;
    }

    // dump help info.
    if (cmd != null && cmd.hasOption("h")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeNode", options, true);
      return;
    }
    // Check FlumeConfiguration file for settings that may cause node to fail.
    nodeConfigChecksOk();

    String nodename = NetUtils.localhost(); // default to local host name.
    if (cmd != null && cmd.hasOption("n")) {
      // select a different name, allow for multiple processes configured
      // differently on same node.
      nodename = cmd.getOptionValue("n");
    }

    boolean startHttp = false;
    if (cmd != null && !cmd.hasOption("s")) {
      // no -s option, start the local status server
      startHttp = true;
    }

    boolean oneshot = false;
    if (cmd != null && cmd.hasOption("1")) {
      oneshot = true;
    }

    // Instantiate the flume node.
    FlumeConfiguration conf = FlumeConfiguration.get();
    FlumeNode flume = new FlumeNode(nodename, conf, startHttp, oneshot);

    flume.start();

    // load an initial configuration from command line
    if (cmd != null && cmd.hasOption("c")) {
      String spec = cmd.getOptionValue("c");
      LOG.info("Loading spec from command line: '" + spec + "'");

      try {
        Context ctx = new LogicalNodeContext(nodename, nodename);
        Map<String, Pair<String, String>> cfgs = FlumeBuilder.parseConf(ctx,
            spec);
        Pair<String, String> node = cfgs.get(nodename);
        flume.nodesMan.spawn(nodename, node.getLeft(), node.getRight());
      } catch (Exception e) {
        LOG.warn("Caught exception loading node:" + e.getMessage());
        LOG.debug("Exception: ", e);
        if (oneshot) {
          System.exit(0); // exit cleanly
        }
      }

    } else {
      // default to null configurations.
      try {
        flume.nodesMan.spawn(nodename, "null", "null");
      } catch (FlumeSpecException e) {
        LOG.error("This should never happen", e);
      } catch (IOException e) {
        LOG.error("Caught exception loading node", e);
      }
    }

    if (cmd != null && cmd.hasOption("m")) {
      // setup memory use monitor
      LOG.info("Setup hard exit on memory exhaustion");
      MemoryMonitor.setupHardExitMemMonitor(FlumeConfiguration.get()
          .getAgentMemoryThreshold());
    }
    // hangout, waiting for other agent thread to exit.
  }

  public static void main(String[] argv) {
    try {
      setup(argv);
    } catch (Exception e) {
      LOG.error("Aborting: Unexpected problem with environment."
          + e.getMessage(), e);
      System.exit(-1);
    }
  }

  public WALManager getWalManager() {
    // default to physical node's wal manager
    synchronized (walMans) {
      return walMans.get(getPhysicalNodeName());
    }
  }

  public WALManager getWalManager(String walnode) {
    synchronized (walMans) {
      if (walnode == null) {
        return getWalManager();
      }
      return walMans.get(walnode);
    }
  }

  public WALManager addWalManager(String walnode) {
    Preconditions.checkArgument(walnode != null);
    FlumeConfiguration conf = FlumeConfiguration.get();
    WALManager wm = new NaiveFileWALManager(new File(new File(conf
        .getAgentLogsDir()), walnode));
    synchronized (walMans) {
      walMans.put(walnode, wm);
      return wm;
    }
  }

  /**
   * Atomically gets an existing dfo for the given node or creates a new one.
   */
  public WALManager getAddWALManager(String dfonode) {
    synchronized (failoverMans) {
      WALManager walman = getWalManager(dfonode);

      if (walman == null) {
        walman = addWalManager(dfonode);
      }
      return walman;
    }
  }

  public DiskFailoverManager getDFOManager() {
    synchronized (failoverMans) {
      return failoverMans.get(getPhysicalNodeName());
    }
  }

  public DiskFailoverManager getDFOManager(String dfonode) {
    synchronized (failoverMans) {
      if (dfonode == null) {
        return getDFOManager();
      }
      return failoverMans.get(dfonode);
    }
  }

  public DiskFailoverManager addDFOManager(String dfonode) {
    Preconditions.checkArgument(dfonode != null);
    FlumeConfiguration conf = FlumeConfiguration.get();
    DiskFailoverManager wm = new NaiveFileFailoverManager(new File(new File(
        conf.getAgentLogsDir()), dfonode));
    synchronized (failoverMans) {
      failoverMans.put(dfonode, wm);
      return wm;
    }
  }

  /**
   * Atomically gets an existing dfo for the given node or creates a new one.
   */
  public DiskFailoverManager getAddDFOManager(String dfonode) {
    synchronized (failoverMans) {
      DiskFailoverManager dfoman = getDFOManager(dfonode);

      if (dfoman == null) {
        dfoman = addDFOManager(dfonode);
      }
      return dfoman;
    }
  }

  public LogicalNodeManager getLogicalNodeManager() {
    return nodesMan;
  }

  // TODO (jon) rename when liveness manager renamed
  public LivenessManager getLivenessManager() {
    return liveMan;
  }

  @Override
  public String getName() {
    return PHYSICAL_NODE_REPORT_PREFIX + this.getPhysicalNodeName();
  }

  @Override
  public ReportEvent getReport() {
    ReportEvent node = new ReportEvent(getName());
    node.setLongMetric(R_NUM_LOGICAL_NODES, this.getLogicalNodeManager()
        .getNodes().size());
    node.hierarchicalMerge(nodesMan.getName(), nodesMan.getReport());
    if (getAckChecker() != null) {
      node.hierarchicalMerge(getAckChecker().getName(), getAckChecker()
          .getReport());
    }
    // TODO (jon) LivenessMan
    // TODO (jon) rpcMan

    return node;
  }

  public String getPhysicalNodeName() {
    return physicalNodeName;
  }
}
