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
import java.lang.reflect.Method;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.cloudera.flume.handlers.debug.ChokeManager;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.reporter.MasterReportPusher;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.util.FlumeVMInfo;
import com.cloudera.flume.util.SystemInfo;
import com.cloudera.util.CheckJavaVersion;
import com.cloudera.util.FileUtil;
import com.cloudera.util.InternalHttpServer;
import com.cloudera.util.NetUtils;
import com.cloudera.util.Pair;
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
  static final Logger LOG = LoggerFactory.getLogger(FlumeNode.class);
  final static String PHYSICAL_NODE_REPORT_PREFIX = "pn-";
  static final String R_NUM_LOGICAL_NODES = "Logical nodes";

  // hook for jsp/web display
  private static FlumeNode instance;

  // run mode for this node.
  final boolean startHttp; // should this node start its own http status server
  private InternalHttpServer http = null;

  private FlumeVMInfo vmInfo;
  private SystemInfo sysInfo;
  private final LivenessManager liveMan;
  private MasterRPC rpcMan;
  private LogicalNodeManager nodesMan;

  private ReportManager simpleReportManager = new ReportManager("simple");
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

  private final ChokeManager chokeMan;

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
    // As this is only for the testing puposes, just initialize the physical
    // node limit to Max Int.
    this.chokeMan = new ChokeManager();
    this.vmInfo = new FlumeVMInfo(PHYSICAL_NODE_REPORT_PREFIX
        + this.physicalNodeName + ".");

    this.reportPusher = new MasterReportPusher(FlumeConfiguration.get(),
        simpleReportManager, rpcMan);
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
      this.reportPusher = new MasterReportPusher(conf, simpleReportManager,
          rpcMan);

    } else {
      this.liveMan = null;
      this.reportPusher = null;
    }

    // initializing ChokeController
    this.chokeMan = new ChokeManager();

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
      String basepath = FlumeConfiguration.getFlumeHome();
      if (basepath == null) {
        LOG.warn("FLUME_HOME not set, potential for odd behavior!");
      }
      File base = new File(basepath, webPath);
      webPath = base.getAbsolutePath();
    }
    return webPath;
  }

  /**
   * This also implements the Apache Commons Daemon interface's start
   */
  synchronized public void start() {
    FlumeConfiguration conf = FlumeConfiguration.get();

    // the simple report interface
    simpleReportManager.add(vmInfo);
    simpleReportManager.add(sysInfo);
    simpleReportManager.add(new Reportable() {

      @Override
      public String getName() {
        return FlumeNode.this.getName();
      }

      @Override
      public ReportEvent getMetrics() {
        return FlumeNode.this.getReport();
      }

      @Override
      public Map<String, Reportable> getSubMetrics() {
        return ReportUtil.noChildren();
      }
    });

    // the full report interface
    ReportManager.get().add(vmInfo);
    ReportManager.get().add(sysInfo);
    ReportManager.get().add(this);

    if (startHttp) {
      try {
        http = new InternalHttpServer();

        http.setBindAddress("0.0.0.0");
        http.setPort(conf.getNodeStatusPort());
        http.setWebappDir(new File(conf.getNodeWebappRoot()));
        http.setScanForApps(true);

        http.start();
      } catch (Throwable t) {
        LOG.error("Unexpected exception/error thrown! " + t.getMessage(), t);
      }
    }

    if (reportPusher != null) {
      reportPusher.start();
    }

    if (liveMan != null) {
      liveMan.start();
    }

    if (chokeMan != null) {
      // JVM exits if only daemons threads remain.
      chokeMan.setDaemon(true);
      chokeMan.start();
    }

  }

  /**
   * This also implements the Apache Commons Daemon interface's stop
   */
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

    if (chokeMan != null) {
      chokeMan.halt();
    }

  }

  /**
   * This also implements the Apache Commons Daemon interface's init
   */
  public void init(String[] args) {
    try {
      setup(args);
    } catch (IOException ioe) {
      LOG.error("Failed to init Flume Node", ioe);
    }
  }

  /**
   * This also implements the Apache Commons Daemon interface's destroy
   */
  public void destroy() {
    stop(); // I think this is ok.
  }

  /**
   * This method is currently called by the JSP to display node information.
   */
  @Deprecated
  public String report() {
    return ReportUtil.getFlattenedReport(this).toHtml();
  }

  public WALAckManager getAckChecker() {
    if (liveMan == null)
      return null;
    return liveMan.getAckChecker();
  }

  public AckListener getCollectorAckListener() {
    return collectorAck;
  }

  public static void logVersion(Logger log) {
    log.info("Flume " + VersionInfo.getVersion());
    log.info(" rev " + VersionInfo.getRevision());
    log.info("Compiled  on " + VersionInfo.getDate());
  }

  public static void logEnvironment(Logger log) {
    Properties props = System.getProperties();
    for (Entry<Object, Object> p : props.entrySet()) {
      log.info("System property " + p.getKey() + "=" + p.getValue());
    }
  }

  /**
   * This function checks the agent logs dir to make sure that the process has
   * the ability to the directory if necessary, that the path if it does exist is
   * a directory, and that it can in fact create files inside of the directory.
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
        LOG.warn("Log directory is writing inside of /tmp.  This data may not survive reboot!");
        break;
      }
      cur = cur.getParentFile();
    }
  }

  /**
   * Returns a Flume Node with settings from specified command line parameters.
   * (See usage for instructions)
   * 
   * @param argv
   * @return
   * @throws IOException
   */
  public static FlumeNode setup(String[] argv) throws IOException {
    logVersion(LOG);
    logEnvironment(LOG);
    // Make sure the Java version is not older than 1.6
    if (!CheckJavaVersion.isVersionOk()) {
      LOG.error("Exiting because of an old Java version or Java version in bad format");
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
        "Have flume hard exit if in likely GC thrash situation");
    options.addOption("h", false, "Print help information");
    options.addOption("v", false, "Print version information");
    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeNode", options, true);
      return null;
    }

    // dump version info only
    if (cmd != null && cmd.hasOption("v")) {
      return null;
    }

    // dump help info.
    if (cmd != null && cmd.hasOption("h")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeNode", options, true);
      return null;
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

    FormatFactory.loadOutputFormatPlugins();

    // Instantiate the flume node.
    FlumeConfiguration conf = FlumeConfiguration.get();

    FlumeNode flume = new FlumeNode(nodename, conf, startHttp, oneshot);

    flume.start();

    // load an initial configuration from command line
    if (cmd != null && cmd.hasOption("c")) {
      String spec = cmd.getOptionValue("c");
      LOG.info("Loading spec from command line: '" + spec + "'");

      try {
        // TODO the first one should be physical node name
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

    try {
      tryKerberosLogin();
    } catch (IOException ioe) {
      LOG.error("Failed to kerberos login.", ioe);
    }

    // hangout, waiting for other agent thread to exit.
    return flume;
  }

  /**
   * This should attempts to kerberos login via a keytab if security is enabled
   * in hadoop.
   * 
   * This should be able to support multiple hadoop clusters as long as the
   * particular principal is allowed on multiple clusters.
   * 
   * To preserve compatibility with non security enhanced hdfs, we use
   * reflection on various UserGroupInformation and SecurityUtil related method
   * calls.
   */
  @SuppressWarnings("unchecked")
  static void tryKerberosLogin() throws IOException {

    /*
     * UserGroupInformation is in hadoop 0.18
     * UserGroupInformation.isSecurityEnabled() not in pre security API.
     * 
     * boolean useSec = UserGroupInformation.isSecurityEnabled();
     */
    boolean useSec = false;

    try {
      Class<UserGroupInformation> c = UserGroupInformation.class;
      // static call, null this obj
      useSec = (Boolean) c.getMethod("isSecurityEnabled").invoke(null);
    } catch (Exception e) {
      LOG.warn("Flume is using Hadoop core "
          + org.apache.hadoop.util.VersionInfo.getVersion()
          + " which does not support Security / Authentication: "
          + e.getMessage());
      return;
    }

    LOG.info("Hadoop Security enabled: " + useSec);
    if (!useSec) {
      return;
    }

    // At this point we know we are using a hadoop library that is kerberos
    // enabled.

    // attempt to load kerberos information for authenticated hdfs comms.
    String principal = FlumeConfiguration.get().getKerberosPrincipal();
    String keytab = FlumeConfiguration.get().getKerberosKeytab();
    LOG.info("Kerberos login as " + principal + " from " + keytab);

    try {
      /*
       * SecurityUtil not present pre hadoop 20.2
       * 
       * SecurityUtil.login not in pre-security Hadoop API
       * 
       * // Keytab login does not need to auto refresh
       * 
       * SecurityUtil.login(FlumeConfiguration.get(),
       * FlumeConfiguration.SECURITY_KERBEROS_KEYTAB,
       * FlumeConfiguration.SECURITY_KERBEROS_PRINCIPAL);
       */
      Class c = Class.forName("org.apache.hadoop.security.SecurityUtil");
      // get method login(Configuration, String, String);
      Method m = c.getMethod("login", Configuration.class, String.class,
          String.class);
      m.invoke(null, FlumeConfiguration.get(),
          FlumeConfiguration.SECURITY_KERBEROS_KEYTAB,
          FlumeConfiguration.SECURITY_KERBEROS_PRINCIPAL);
    } catch (Exception e) {
      LOG.error("Flume failed when attempting to authenticate with keytab "
          + FlumeConfiguration.get().getKerberosKeytab() + " and principal '"
          + FlumeConfiguration.get().getKerberosPrincipal() + "'", e);

      // e.getMessage() comes from hadoop is worthless
      return;
    }

    try {
      /*
       * getLoginUser, getAuthenticationMethod, and isLoginKeytabBased are not
       * in Hadoop 20.2, only kerberized enhanced version.
       * 
       * getUserName is in all 0.18.3+
       * 
       * UserGroupInformation ugi = UserGroupInformation.getLoginUser();
       * LOG.info("Auth method: " + ugi.getAuthenticationMethod());
       * LOG.info(" User name: " + ugi.getUserName());
       * LOG.info(" Using keytab: " +
       * UserGroupInformation.isLoginKeytabBased());
       */

      Class<UserGroupInformation> c2 = UserGroupInformation.class;
      // static call, null this obj
      UserGroupInformation ugi = (UserGroupInformation) c2.getMethod(
          "getLoginUser").invoke(null);
      String authMethod = c2.getMethod("getAuthenticationMethod").invoke(ugi)
          .toString();
      boolean keytabBased = (Boolean) c2.getMethod("isLoginKeytabBased")
          .invoke(ugi);

      LOG.info("Auth method: " + authMethod);
      LOG.info(" User name: " + ugi.getUserName());
      LOG.info(" Using keytab: " + keytabBased);
    } catch (Exception e) {
      LOG.error("Flume was unable to dump kerberos login user"
          + " and authentication method", e);
      return;
    }

  }

  public static void main(String[] argv) {
    try {
      setup(argv);
    } catch (Exception e) {
      LOG.error(
          "Aborting: Unexpected problem with environment." + e.getMessage(), e);
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
    WALManager wm = new NaiveFileWALManager(new File(new File(
        conf.getAgentLogsDir()), walnode));
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

  public ChokeManager getChokeManager() {
    return chokeMan;
  }

  @Override
  public String getName() {
    return PHYSICAL_NODE_REPORT_PREFIX + this.getPhysicalNodeName();
  }

  @Deprecated
  public ReportEvent getReport() {
    ReportEvent node = new ReportEvent(getName());
    node.setLongMetric(R_NUM_LOGICAL_NODES, this.getLogicalNodeManager()
        .getNodes().size());
    node.hierarchicalMerge(nodesMan.getName(), nodesMan.getReport());
    if (getAckChecker() != null) {
      node.hierarchicalMerge(getAckChecker().getName(), getAckChecker()
          .getMetrics());
    }
    return node;
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent node = new ReportEvent(getName());
    node.setLongMetric(R_NUM_LOGICAL_NODES, this.getLogicalNodeManager()
        .getNodes().size());
    return node;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(nodesMan.getName(), nodesMan);

    WALAckManager ack = getAckChecker();
    if (ack != null) {
      map.put(ack.getName(), ack);
    }

    map.put("jvmInfo", vmInfo);
    map.put("sysInfo", sysInfo);

    // TODO (jon) LivenessMan
    // TODO (jon) rpcMan

    return map;
  }

  public String getPhysicalNodeName() {
    return physicalNodeName;
  }

  public SystemInfo getSystemInfo() {
    return sysInfo;
  }

  public FlumeVMInfo getVMInfo() {
    return vmInfo;
  }
}
