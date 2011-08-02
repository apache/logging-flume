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
package com.cloudera.flume.conf;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This is the configuration file for flume. It uses hadoop's xml schema for
 * properties. The default configuration is in $FLUME_HOME/flume-conf.xml and is
 * a singleton.
 * 
 * NOTE: The logging system gets configuration information from this file and
 * thus depends on this being initialized. So no logging events are allowed when
 * this is being initialized. If logger is used, an infinite recursion happens
 * and causes a stack overflow.
 */
public class FlumeConfiguration extends Configuration {
  final protected static Logger LOG = Logger
      .getLogger(FlumeConfiguration.class);
  private static FlumeConfiguration singleton;

  /**
   * Returns the 'FLUME_HOME' location. Taken in order of precedence:
   * - Java system property 'flume.home'
   * - $FLUME_HOME in the environment.
   * - null if neither of these are set.
   */
  public static String getFlumeHome() {
    String flumeHome = System.getProperty("flume.home");
    if (null == flumeHome) {
      flumeHome = System.getenv("FLUME_HOME");
    }

    if (null == flumeHome) {
      LOG.warn("-Dflume.home and $FLUME_HOME both unset");
    }

    return flumeHome;
  }

  /**
   * Returns the 'FLUME_CONF_DIR' location. Taken in order of precedence:
   * - Java system property 'flume.conf.dir'
   * - $FLUME_CONF_DIR in the environment
   * - getFlumeHome()/conf
   * - ./conf
   */
  public static String getFlumeConfDir() {
    String flumeConfDir = System.getProperty("flume.conf.dir");
    if (null == flumeConfDir) {
      flumeConfDir = System.getenv("FLUME_CONF_DIR");
    }

    if (null == flumeConfDir) {
      String flumeHome = getFlumeHome();
      if (null != flumeHome) {
        flumeConfDir = new File(flumeHome, "conf").toString();
      } else {
        flumeConfDir = "./conf";
      }
    }

    return flumeConfDir;
  }

  public synchronized static FlumeConfiguration get() {
    if (singleton == null)
      singleton = new FlumeConfiguration();
    return singleton;
  }

  protected FlumeConfiguration() {
    this(true);
  }

  /**
   * Resets the singleton configuration to empty and returns it. Should be used
   * for testing only - this is used to isolate different test configurations.
   */
  public synchronized static FlumeConfiguration createTestableConfiguration() {
    singleton = new FlumeConfiguration(false);
    return get();
  }

  protected FlumeConfiguration(boolean loadDefaults) {
    super();
    if (loadDefaults) {
      Path home = null;
      String flumeHome = getFlumeHome();
      if (flumeHome == null) {
        home = new Path(".");
      } else {
        home = new Path(flumeHome);
      }
      Path conf = new Path(getFlumeConfDir());
      LOG.info("Loading configurations from " + conf);
      super.addResource(new Path(conf, "flume-conf.xml"));
      super.addResource(new Path(conf, "flume-site.xml"));
    }
  }

  // Default values
  static public final int DEFAULT_HEARTBEAT_PORT = 35872;
  static public final int DEFAULT_GOSSIP_PORT = 57890;
  static public final int DEFAULT_ADMIN_PORT = 35873;
  static public final int DEFAULT_HTTP_PORT = 35871;
  static public final int DEFAULT_REPORT_SERVER_PORT = 45678;

  public static final int DEFAULT_ZK_CLIENT_PORT = 3181;
  public static final int DEFAULT_ZK_SERVER_QUORUM_PORT = 3182;
  public static final int DEFAULT_ZK_SERVER_ELECTION_PORT = 3183;

  // Default sink / source variables
  static public final int DEFAULT_SCRIBE_SOURCE_PORT = 1463;

  // Watch dog parameters
  public final static String WATCHDOG_MAX_RESTARTS = "watchdog.restarts.max";

  // Agent parameters
  public final static String AGENT_LOG_DIR_NEW = "flume.agent.logdir";
  public final static String AGENT_LOG_MAX_AGE = "flume.agent.logdir.maxage";
  public static final String AGENT_LOG_ACKED_RETRANSMIT_AGE = "flume.agent.logdir.retransmit";

  public final static String NODE_STATUS_PORT = "flume.node.status.port";
  public static final String AGENT_FAILOVER_INITIAL_BACKOFF = "flume.agent.failover.backoff.initial";
  public static final String AGENT_FAILOVER_MAX_BACKOFF = "flume.agent.failover.backoff.max";
  public static final String AGENT_FAILOVER_MAX_CUMULATIVE_BACKOFF = "flume.agent.failover.backoff.cumulativemax";
  public static final String AGENT_HEARTBEAT_BACKOFF = "flume.agent.heartbeat.backoff";
  public static final String AGENT_MEMTHRESHOLD = "flume.agent.mem.threshold";
  public static final String AGENT_MULTIMASTER_MAXRETRIES = "flume.agent.multimaster.maxretries";
  public static final String AGENT_MULTIMASTER_RETRYBACKOFF = "flume.agent.multimaster.retrybackoff";

  // Flow options
  public static final String DEFAULT_FLOW_NAME = "flume.flow.default.name";

  // Sink/source default options
  public static final String POLLER_QUEUESIZE = "flume.poller.queuesize";
  public static final String THRIFT_QUEUESIZE = "flume.thrift.queuesize";
  public static final String THRIFT_CLOSE_MAX_SLEEP = "flume.thrift.close.maxsleep";
  public static final String INSISTENTOPEN_INIT_BACKOFF = "flume.inisistentOpen.init.backoff";
  public static final String HISTORY_DEFAULTPERIOD = "flume.countHistory.period";
  public static final String HISTORY_MAXLENGTH = "flume.history.maxlength";
  public static final String TAIL_POLLPERIOD = "flume.tail.pollperiod";
  public static final String EVENT_RPC_TYPE = "flume.event.rpc";

  // Collector parameters
  public final static String COLLECTOR_EVENT_HOST = "flume.collector.event.host";
  public final static String COLLECTOR_EVENT_PORT = "flume.collector.event.port";
  public static final String COLLECTOR_DFS_DIR = "flume.collector.dfs.dir";
  public static final String COLLECTOR_ROLL_MILLIS = "flume.collector.roll.millis";
  public static final String COLLECTOR_OUTPUT_FORMAT = "flume.collector.output.format";
  public static final String COLLECTOR_DFS_COMPRESS_GZIP = "flume.collector.dfs.compress.gzip";
  public static final String COLLECTOR_DFS_COMPRESS_CODEC = "flume.collector.dfs.compress.codec";

  // TODO(henry) move these to flume.master - they now tell the master which
  // interface / port to start up on
  public static final String MASTER_HTTP_PORT = "flume.master.http.port";
  public static final String MASTER_HEARTBEAT_PORT = "flume.master.heartbeat.port";
  public static final String MASTER_HEARTBEAT_SERVERS = "flume.master.heartbeat.servers";
  public static final String MASTER_HEARBEAT_RPC = "flume.master.heartbeat.rpc";

  public static final String CONFIG_HEARTBEAT_PERIOD = "flume.config.heartbeat.period";
  public static final String MASTER_HEARTBEAT_MAX_MISSED = "flume.config.heartbeat.missed.max";
  public static final String NODE_HEARTBEAT_BACKOFF_LIMIT = "flume.node.heartbeat.backoff.ceiling";
  public static final String NODE_HTTP_AUTOFINDPORT = "flume.node.http.autofindport";
  public static final String NODE_CLOSE_TIMEOUT = "flume.node.close.timeout";
  public static final String CONFIG_ADMIN_PORT = "flume.config.admin.port";
  public static final String REPORT_SERVER_PORT = "flume.report.server.port";
  public static final String REPORT_SERVER_RPC_TYPE = "flume.report.server.rpc.type";
  public static final String MASTER_SAVEFILE = "flume.master.savefile";
  public static final String MASTER_SAVEFILE_AUTOLOAD = "flume.master.savefile.autoload";

  // reporter parameters
  public static final String REPORTER_POLLER_PERIOD = "flume.reporter.poller.period";

  // security parameters
  public static final String SECURITY_KERBEROS_PRINCIPAL = "flume.security.kerberos.principal";
  public static final String SECURITY_KERBEROS_KEYTAB = "flume.security.kerberos.keytab";

  public static final String FLURKER_ENCODING = "flume.irc.encoding";

  public static final String TWITTER_STREAM_URL = "flume.twitter.url";
  public static final String TWITTER_USER = "flume.twitter.username";
  public static final String TWITTER_PW = "flume.twitter.password";

  public static final String SCRIBE_SOURCE_PORT = "flume.scribe.source.port";

  public static final String WEBAPPS_PATH = "flume.webapps.path";

  // 80% of the time, this is all you need to set
  public static final String MASTER_SERVERS = "flume.master.servers";

  public static final String MASTER_STORE = "flume.master.store";
  public static final String MASTER_SERVER_ID = "flume.master.serverid";

  // List of masters and their gossip ports - used to override MASTER_SERVERS
  // where necessary
  public static final String MASTER_GOSSIP_SERVERS = "flume.master.gossip.servers";
  public static final String MASTER_GOSSIP_PERIOD_MS = "flume.master.gossip.period";
  public static final String MASTER_GOSSIP_PORT = "flume.master.gossip.port";

  // ZooKeeper bits and pieces
  public static final String MASTER_ZK_LOGDIR = "flume.master.zk.logdir";
  public static final String MASTER_ZK_CLIENT_PORT = "flume.master.zk.client.port";
  public static final String MASTER_ZK_SERVER_QUORUM_PORT = "flume.master.zk.server.quorum.port";
  public static final String MASTER_ZK_SERVER_ELECTION_PORT = "flume.master.zk.server.election.port";
  public static final String MASTER_ZK_SERVERS = "flume.master.zk.servers";
  public static final String MASTER_ZK_USE_EXTERNAL = "flume.master.zk.use.external";

  // These are configuration variables that are passed straight to ZK
  // 99% of installations shouldn't need most of them
  public static final String ZK_TICK_TIME = "flume.zk.ticktime";
  public static final String ZK_INIT_LIMIT = "flume.zk.initlimit";
  public static final String ZK_SYNC_LIMIT = "flume.zk.synclimit";

  public static final String EVENT_MAX_SIZE = "flume.event.max.size.bytes";

  public static final String GANGLIA_SERVERS = "flume.ganglia.servers";

  public static final String HIVE_HOST = "flume.hive.host";
  public static final String HIVE_PORT = "flume.hive.port";
  public static final String HIVE_USER = "flume.hive.user";
  public static final String HIVE_PW = "flume.hive.userpw";

  public static final String PLUGIN_CLASSES = "flume.plugin.classes";

  // Options for RPC type
  public static final String RPC_TYPE_THRIFT = "THRIFT";
  public static final String RPC_TYPE_AVRO = "AVRO";

  /**
   * Returns true if there is more than one server in MASTER_SERVERS.
   */
  public boolean getMasterIsDistributed() {
    return getMasterServers().split(",").length > 1;
  }

  /**
   * Returns the kind of store that the master should use. Using "memory" in
   * conjunction with a distributed setting is an error and the client should
   * pick up on that.
   */
  public String getMasterStore() {
    return get(MASTER_STORE, "zookeeper");
  }

  /**
   * Get a list in host:clientport:serverport format of servers to connect to
   * for ZK. If MASTER_ZK_SERVERS is set, return that. Otherwise, return the
   * servers from MASTER_SERVERS with MASTER_ZK_CLIENT_PORT and
   * MASTER_ZK_SERVER_PORT as default values.
   */
  public String getMasterZKServers() {
    String servers = get(MASTER_ZK_SERVERS, null);
    if (servers != null) {
      return servers;
    }

    String[] hosts = getMasterServers().split(",");
    int clientport = getMasterZKClientPort();
    int quorumport = getMasterZKServerQuorumPort();
    int electionport = getMasterZKServerElectionPort();
    List<String> l = Arrays.asList(hosts);
    Iterator<String> iter = l.iterator();
    StringBuilder builder = new StringBuilder();
    while (iter.hasNext()) {
      builder.append(iter.next() + ":" + clientport + ":" + quorumport + ":"
          + electionport);
      if (iter.hasNext()) {
        builder.append(',');
      }
    }
    return builder.toString();
  }

  /**
   * Returns the string that a client should use when connecting to ZooKeeper.
   * List of hostname:clientport comma-separated pairs. If no servers
   * configured, will be empty.
   */
  public String getMasterZKConnectString() {
    String servers = getMasterZKServers();
    String[] hosts = servers.split(",");
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < hosts.length; ++i) {
      hosts[i] = hosts[i].trim();

      String[] parts = hosts[i].split(":");
      builder.append(parts[0] + ":" + parts[1]);
      if (i < hosts.length - 1) {
        builder.append(",");
      }
    }
    return builder.toString();
  }

  /**
   * The client port that the in-process ZK starts on.
   * 
   * If it is set, return that.
   * 
   * If MASTER_ZK_SERVERS is set, then we return the clientport in that string
   * corresponding to our serverid.
   * 
   * Otherwise return DEFAULT_ZK_CLIENT_PORT.
   * 
   */
  public int getMasterZKClientPort() {
    String clientport = get(MASTER_ZK_CLIENT_PORT, null);
    if (clientport != null) {
      return Integer.parseInt(clientport);
    }

    // clientport is not set, try and guess from MASTER_ZK_SERVERS if it's set
    // We can't call getMasterZKServers because that might call into this method
    // itself...
    String servers = get(MASTER_ZK_SERVERS, null);
    if (servers == null) {
      return DEFAULT_ZK_CLIENT_PORT;
    }

    // MASTER_ZK_SERVERS is set - split it and guess at our client port
    List<String> serverList = Arrays.asList(servers.split(","));
    int serverid = getMasterServerId();
    Preconditions.checkState(serverid < serverList.size(),
        "Serverid is out of range: " + serverid);

    String[] server = serverList.get(serverid).split(":");
    Preconditions.checkState(server.length == 4, "Server spec "
        + serverList.get(serverid) + " is ill-formed");
    return Integer.parseInt(server[1].trim());
  }

  /**
   * The server quorum port that the in-process ZK starts on.
   * 
   * If it is set, return that.
   * 
   * If MASTER_ZK_SERVERS is set, then we return the quorumport in that string
   * corresponding to our serverid.
   * 
   * Otherwise return DEFAULT_ZK_SERVER_QUORUM_PORT, the default.
   * 
   */
  public int getMasterZKServerQuorumPort() {
    String quorumport = get(MASTER_ZK_SERVER_QUORUM_PORT, null);
    if (quorumport != null) {
      return Integer.parseInt(quorumport);
    }

    // quorumport is not set, try and guess from MASTER_ZK_SERVERS if it's set
    // We can't call getMasterZKServers because that might call into this method
    // itself...
    String servers = get(MASTER_ZK_SERVERS, null);
    if (servers == null) {
      return DEFAULT_ZK_SERVER_QUORUM_PORT;
    }

    // MASTER_ZK_SERVERS is set - split it and guess at our server port
    List<String> serverList = Arrays.asList(servers.split(","));
    int serverid = getMasterServerId();
    Preconditions.checkState(serverid < serverList.size(),
        "Serverid is out of range: " + serverid);

    String[] server = serverList.get(serverid).split(":");
    Preconditions.checkState(server.length == 4, "Server spec "
        + serverList.get(serverid) + " is ill-formed");
    return Integer.parseInt(server[2].trim());
  }

  /**
   * The server election port that the in-process ZK starts on.
   * 
   * If it is set, return that.
   * 
   * If MASTER_ZK_SERVERS is set, then we return the electionport in that string
   * corresponding to our serverid.
   * 
   * Otherwise return DEFAULT_ZK_SERVER_ELECTION_PORT, the default.
   * 
   */
  public int getMasterZKServerElectionPort() {
    String electionport = get(MASTER_ZK_SERVER_ELECTION_PORT, null);
    if (electionport != null) {
      return Integer.parseInt(electionport);
    }

    // electionport is not set, try and guess from MASTER_ZK_SERVERS if it's set
    // We can't call getMasterZKServers because that might call into this method
    // itself...
    String servers = get(MASTER_ZK_SERVERS, null);
    if (servers == null) {
      return DEFAULT_ZK_SERVER_ELECTION_PORT;
    }

    // MASTER_ZK_SERVERS is set - split it and guess at our server port
    List<String> serverList = Arrays.asList(servers.split(","));
    int serverid = getMasterServerId();
    Preconditions.checkState(serverid < serverList.size(),
        "Serverid is out of range: " + serverid);

    String[] server = serverList.get(serverid).split(":");
    Preconditions.checkState(server.length == 4, "Server spec "
        + serverList.get(serverid) + " is ill-formed");
    return Integer.parseInt(server[3].trim());
  }

  public boolean getMasterZKUseExternal() {
    return getBoolean(MASTER_ZK_USE_EXTERNAL, false);
  }

  /**
   * Where should ZK server store its persistent logs? (Shouldn't be in tmp!)
   */
  public String getMasterZKLogDir() {
    return get(MASTER_ZK_LOGDIR, "/tmp/flume/master/zk");
  }

  /**
   * The amount of time, in ms, that a 'tick' takes in ZK.
   */
  public int getMasterZKTickTime() {
    return getInt(ZK_TICK_TIME, 2000);
  }

  /**
   * Amount of time, in ticks, to allow followers to connect and sync to a
   * leader.
   * 
   * @return
   */
  public int getMasterZKInitLimit() {
    return getInt(ZK_INIT_LIMIT, 10);
  }

  /**
   * Amount of time, in ticks, that ZK followers can take to sync up with the
   * leader.
   */
  public int getMasterZKSyncLimit() {
    return getInt(ZK_SYNC_LIMIT, 5);
  }

  /**
   * For distributed ZK, we need to know which server we are.
   */
  public int getMasterServerId() {
    return getInt(MASTER_SERVER_ID, 0);
  }

  public String getAgentLogsDir() {
    return get(AGENT_LOG_DIR_NEW, "/tmp/flume/agent");
  }

  public long getAgentLogMaxAge() {
    return getLong(AGENT_LOG_MAX_AGE, 10000);
  }

  /**
   * Maximum number of application restarts per minute
   */
  public int getMaxRestartsPerMin() {
    return getInt(WATCHDOG_MAX_RESTARTS, 5);
  }

  public int getNodeStatusPort() {
    return getInt(NODE_STATUS_PORT, 35862);
  }

  public long getAgentAckedRetransmit() {
    return getLong(AGENT_LOG_ACKED_RETRANSMIT_AGE, 60000);
  }

  /**
   * Heap memory usage threshold (between 0.0 and 1.0) after a forced gc.
   */
  public float getAgentMemoryThreshold() {
    return getFloat(AGENT_MEMTHRESHOLD, 0.95F);
  }

  /**
   * Maximum number of times to retry connecting to all masters.
   */
  public int getAgentMultimasterMaxRetries() {
    return getInt(AGENT_MULTIMASTER_MAXRETRIES, 12);
  }

  /**
   * Amount of time to backoff before attempting to re-contact a master when all
   * fail.
   */
  public int getAgentMultimasterRetryBackoff() {
    return getInt(AGENT_MULTIMASTER_RETRYBACKOFF, 5000);
  }

  public long getFailoverInitialBackoff() {
    return getLong(AGENT_FAILOVER_INITIAL_BACKOFF, 1000);
  }

  public long getFailoverMaxSingleBackoff() {
    return getLong(AGENT_FAILOVER_MAX_BACKOFF, 60000);
  }

  public long getFailoverMaxCumulativeBackoff() {
    return getLong(AGENT_FAILOVER_MAX_CUMULATIVE_BACKOFF, Integer.MAX_VALUE);
  }

  /**
   * Max queue size for a poller source.
   */
  public int getPollerQueueSize() {
    return getInt(POLLER_QUEUESIZE, 100);
  }

  /**
   * Max queue size for a poller source.
   */
  public int getThriftQueueSize() {
    return getInt(THRIFT_QUEUESIZE, 1000);
  }

  /**
   * Initial backoff in mills after a failed open attempt in an insistentOpen
   * decorator
   */
  public long getInsistentOpenInitBackoff() {
    return getLong(INSISTENTOPEN_INIT_BACKOFF, 1000);
  }

  /**
   * Period for to reset counters in millis
   */
  public long getHistoryDefaultPeriod() {
    return getLong(HISTORY_DEFAULTPERIOD, 1000);
  }

  /**
   * Maximum number of discrete history events to hold onto for reporting.
   */
  public long getHistoryMaxLength() {
    return getLong(HISTORY_MAXLENGTH, 300);
  }

  /**
   * Polling period in millis for tail to check a file.
   * 
   * Currently we poll to see if a file has changed and this allows the user to
   * tweak the frequency
   */
  public long getTailPollPeriod() {
    return getLong(TAIL_POLLPERIOD, 1000);
  }

  public String getCollectorHost() {
    return get(COLLECTOR_EVENT_HOST, "localhost");
  }

  public int getCollectorPort() {
    // FLUME = 35863 (flume on the telephone)
    return getInt(COLLECTOR_EVENT_PORT, 35853);
  }

  /**
   * This returns the type of RPC mechanism (Thrift or Avro) chosen for the
   * FlumeEventServer.
   */
  public String getEventRPC() {
    String[] validRPCProtocols = { RPC_TYPE_AVRO, RPC_TYPE_THRIFT };
    String entered = get(EVENT_RPC_TYPE, RPC_TYPE_THRIFT).toUpperCase();
    for (String prot : validRPCProtocols) {
      if (entered.equals(prot)) {
        return prot;
      }
    }
    // defaulting to Thrift with a polite warning
    LOG.warn("event.rpc.type incorrectly defined, should be either"
        + " \"THRIFT\" or \"AVRO\".  Defaulting to \"THRIFT\"");
    return RPC_TYPE_THRIFT;
  }

  public String getCollectorDfsDir() {
    return get(COLLECTOR_DFS_DIR, "file://tmp/flume/collected");
  }

  @Deprecated
  public boolean getCollectorDfsCompressGzipStatus() {
    return getBoolean(COLLECTOR_DFS_COMPRESS_GZIP, false);
  }

  public String getCollectorDfsCompressCodec() {
    return get(COLLECTOR_DFS_COMPRESS_CODEC, "None");
  }

  public long getCollectorRollMillis() {
    return getLong(COLLECTOR_ROLL_MILLIS, 30000);
  }

  /**
   * This is the list of masters that agent nodes will connect to
   */
  public String getMasterServers() {
    String svrs = get(MASTER_SERVERS, "localhost");

    // check for illegal ':'s in the servers; truncate stuff after the ':'
    String[] hosts = svrs.split(",");
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < hosts.length; ++i) {
      hosts[i] = hosts[i].trim();

      String[] parts = hosts[i].split(":");
      builder.append(parts[0]);
      if (parts.length > 1) {
        LOG.warn("Master Server's should not have list ports but host '"
            + hosts[i] + " 'specified ports! ");
      }

      if (i < hosts.length - 1) {
        builder.append(",");
      }
    }

    return builder.toString();
  }

  /**
   * Return a list of server:port pairs for heartbeat purposes
   */
  public String getMasterHeartbeatServers() {
    String servers = get(MASTER_HEARTBEAT_SERVERS, null);
    if (servers != null) {
      return servers;
    }

    String[] hosts = getMasterServers().split(",");
    int heartbeatport = getMasterHeartbeatPort();
    List<String> l = Arrays.asList(hosts);
    Iterator<String> iter = l.iterator();
    StringBuilder builder = new StringBuilder();
    while (iter.hasNext()) {
      builder.append(iter.next() + ":" + heartbeatport);
      if (iter.hasNext()) {
        builder.append(',');
      }
    }
    return builder.toString();
  }

  /**
   * Utility function to retrieve master hosts as a list of host / port pairs
   * used only on the client!
   */
  public List<Pair<String, Integer>> getMasterHeartbeatServersList() {
    String[] hosts = getMasterHeartbeatServers().split(",");
    List<Pair<String, Integer>> ret = new ArrayList<Pair<String, Integer>>(
        hosts.length);
    for (String s : hosts) {
      String[] parts = s.split(":");
      ret.add(new Pair<String, Integer>(parts[0], Integer.parseInt(parts[1])));
    }
    return ret;
  }

  public int getMasterHttpPort() {
    return getInt(MASTER_HTTP_PORT, DEFAULT_HTTP_PORT);
  }

  /**
   * The port on which to start a ReportServer
   */
  public int getReportServerPort() {
    return getInt(REPORT_SERVER_PORT, DEFAULT_REPORT_SERVER_PORT);
  }


  /**
   * This returns the type of RPC mechanism (Thrift or Avro) chosen for the
   * FlumeReportServer.
   */
  public String getReportServerRPC() {
    String[] validRPCProtocols = { RPC_TYPE_AVRO, RPC_TYPE_THRIFT };
    String entered = get(REPORT_SERVER_RPC_TYPE, RPC_TYPE_THRIFT).toUpperCase();
    for (String prot : validRPCProtocols) {
      if (entered.equals(prot)) {
        return prot;
      }
    }
    // defaulting to Thrift with a polite warning
    LOG.warn("flume.report.server.rpc.type incorrectly defined, should be either"
        + " \"THRIFT\" or \"AVRO\".  Defaulting to \"THRIFT\"");
    return RPC_TYPE_THRIFT;
  }
  
  /**
   * If MASTER_HEARTBEAT_PORT is set, we use that as our heartbeat port. If not,
   * we look at the list of server:port pairs in MASTER_HEARTBEAT_SERVERS, in
   * particular we look at the MASTER_SERVER_ID'th value
   */
  public int getMasterHeartbeatPort() {
    String port = get(MASTER_HEARTBEAT_PORT, null);
    if (port != null) {
      return Integer.parseInt(port);
    }
    String servers = get(MASTER_HEARTBEAT_SERVERS, null);
    if (servers == null) {
      return DEFAULT_HEARTBEAT_PORT;
    }

    // MASTER_HEARTBEAT_SERVERS is set - split it and guess at our server port
    List<String> serverList = Arrays.asList(servers.split(","));
    int serverid = getMasterServerId();
    Preconditions.checkState(serverid < serverList.size(),
        "Serverid is out of range: " + serverid);

    String[] server = serverList.get(serverid).split(":");
    Preconditions.checkState(server.length == 2, "Server spec "
        + serverList.get(serverid) + " is ill-formed");
    return Integer.parseInt(server[1].trim());
  }

  public String getMasterHeartbeatRPC() {
    String[] validRPCProtocols = { RPC_TYPE_AVRO, RPC_TYPE_THRIFT };
    String entered = get(MASTER_HEARBEAT_RPC, RPC_TYPE_THRIFT).toUpperCase();
    for (String prot : validRPCProtocols) {
      if (entered.equals(prot)) {
        return prot;
      }
    }
    // default
    return RPC_TYPE_THRIFT;
  }

  public int getMasterGossipPeriodMs() {
    return getInt(MASTER_GOSSIP_PERIOD_MS, 1000);
  }

  public int getMasterGossipPort() {
    String port = get(MASTER_GOSSIP_PORT, null);
    if (port != null) {
      return Integer.parseInt(port);
    }
    // gossipport is not set, try and guess from MASTER_GOSSIP_SERVERS if it's
    // set
    // We can't call getMasterGossipServers because that might call into this
    // method
    // itself...
    String servers = get(MASTER_GOSSIP_SERVERS, null);
    if (servers == null) {
      return DEFAULT_GOSSIP_PORT;
    }

    // MASTER_GOSSIP_SERVERS is set - split it and guess at our server port
    List<String> serverList = Arrays.asList(servers.split(","));
    int serverid = getMasterServerId();
    Preconditions.checkState(serverid < serverList.size(),
        "Serverid is out of range: " + serverid);

    String[] server = serverList.get(serverid).split(":");
    Preconditions.checkState(server.length == 2, "Server spec "
        + serverList.get(serverid) + " is ill-formed");
    return Integer.parseInt(server[1].trim());
  }

  /**
   * Returns a comma-separated list of host:port pairs. If not explicitly set,
   * defaults to MASTER_SERVERS, using MASTER_GOSSIP_PORT.
   */
  public String getMasterGossipServers() {
    String ret = get(MASTER_GOSSIP_SERVERS, null);
    if (ret != null) {
      return ret;
    }

    // Otherwise return the list of ensemble hosts and the gossip port
    String[] hosts = getMasterServers().split(",");
    List<String> l = Arrays.asList(hosts);
    Iterator<String> iter = l.iterator();
    StringBuilder builder = new StringBuilder();
    while (iter.hasNext()) {
      builder.append(iter.next() + ":" + getMasterGossipPort());
      if (iter.hasNext()) {
        builder.append(',');
      }
    }
    return builder.toString();
  }

  /**
   * Utility function to return a list of host:port pairs corresponding to all
   * the gossip hosts in a Flume Master ensemble.
   */
  public List<Pair<String, Integer>> getConfigMasterGossipHostsList() {
    String hostString = getMasterGossipServers();
    if (hostString.equals("")) {
      return new ArrayList<Pair<String, Integer>>();
    }
    String[] hosts = hostString.split(",");
    List<Pair<String, Integer>> ret = new ArrayList<Pair<String, Integer>>(
        hosts.length);
    for (String s : hosts) {
      String[] parts = s.split(":");
      ret.add(new Pair<String, Integer>(parts[0], Integer.parseInt(parts[1])));
    }
    return ret;
  }

  public long getConfigHeartbeatPeriod() {
    return getLong(CONFIG_HEARTBEAT_PERIOD, 5000); // millis
  }

  public int getMasterMaxMissedheartbeats() {
    return getInt(MASTER_HEARTBEAT_MAX_MISSED, 10);
  }

  public long getNodeHeartbeatBackoffLimit() {
    return getLong(NODE_HEARTBEAT_BACKOFF_LIMIT, 60000); // 1 minute in millis
  }

  public boolean getNodeAutofindHttpPort() {
    return getBoolean(NODE_HTTP_AUTOFINDPORT, true);
  }

  public int getConfigAdminPort() {
    return getInt(CONFIG_ADMIN_PORT, DEFAULT_ADMIN_PORT);
  }

  public int getConfigReportPort() {
    return getInt(REPORT_SERVER_PORT, DEFAULT_REPORT_SERVER_PORT);
  }

  public String getMasterSavefile() {
    return get(MASTER_SAVEFILE, "conf/current.flume");
  }

  public boolean getMasterSavefileAutoload() {
    return getBoolean(MASTER_SAVEFILE_AUTOLOAD, false);
  }

  public String getKerberosPrincipal() {
    // flume is the default kerberos principal for hdfs writes
    // _HOST is expanded by the hadoop security util to be the local host name.
    return get(SECURITY_KERBEROS_PRINCIPAL, "");
  }

  public String getKerberosKeytab() {
    // mit kerb v5 default keytab path
    return get(SECURITY_KERBEROS_KEYTAB, "/etc/flume/conf/krb5.keytab");
  }

  public long getReporterPollPeriod() {
    return getLong(REPORTER_POLLER_PERIOD, 2000); // millis
  }

  public String getFlurkerEncoding() {
    return get(FLURKER_ENCODING, "UTF-8");
  }

  public String getWebAppsPath() {
    return get(WEBAPPS_PATH, "webapps");
  }

  /**
   * This method loads the configuration, or does a hard exit if loading the
   * configuration fails.
   */
  public static FlumeConfiguration hardExitLoadConfig() {

    try {
      FlumeConfiguration conf = FlumeConfiguration.get();

      // Loading the config is lazy! need to try to get something before an
      // exception is thrown.

      conf.getMaxRestartsPerMin(); // get some random value.
      return conf;
    } catch (RuntimeException parseEx) {
      System.exit(-1);
    }
    return null; // dead code
  }

  public String getTwitterName() {
    return get(TWITTER_USER, "");
  }

  public String getTwitterPW() {
    return get(TWITTER_PW, "");
  }

  public String getTwitterURL() {
    return get(TWITTER_STREAM_URL,
        "http://stream.twitter.com/1/statuses/sample.json");
  }

  public String getDefaultOutputFormat() {
    // Look at FormatFactory for possible values.
    return get(COLLECTOR_OUTPUT_FORMAT, "avrojson");
  }

  public String getGangliaServers() {
    // gmond's default multicast ip and port
    return get(GANGLIA_SERVERS, "239.2.11.71:8649");
  }

  public long getEventMaxSizeBytes() {
    return getLong(EVENT_MAX_SIZE, 32 * 1024); // Default to 32k
  }

  public String getPluginClasses() {
    return get(PLUGIN_CLASSES, "");
  }

  public String getHiveHost() {
    return get(HIVE_HOST, "localhost");
  }

  public int getHivePort() {
    return getInt(HIVE_PORT, 9083); // default port found from meta store
    // presentation.
  }

  public String getHiveUser() {
    return get(HIVE_USER, "hive");
  }

  public String getHiveUserPW() {
    return get(HIVE_PW, "");
  }

  public long getHeartbeatBackoff() {
    return getLong(AGENT_HEARTBEAT_BACKOFF, 1000); // millis
  }

  /**
   * Max milliseconds to back off after a close request. Close will wait at most
   * this amount of time after it sees progress after a close request. * after a
   * close request.
   */
  public long getThriftCloseMaxSleep() {
    return getLong(THRIFT_CLOSE_MAX_SLEEP, 10 * 1000);
  }

  /**
   * The default port for scribe sources to listen on
   */
  public int getScribeSourcePort() {
    return getInt(SCRIBE_SOURCE_PORT, DEFAULT_SCRIBE_SOURCE_PORT);
  }

  /**
   * Returns the default flow name for logical nodes.
   */
  public String getDefaultFlowName() {
    return get(DEFAULT_FLOW_NAME, "default-flow");
  }

  /**
   * Returns the current FlumeConfiguration as an HTML string
   */
  public String toHtml() {
    Iterator<Entry<String, String>> iter = iterator();
    ArrayList<String> keys = new ArrayList<String>();
    while (iter.hasNext()) {
      Entry<String, String> e = iter.next();
      keys.add(e.getKey());
    }
    Collections.sort(keys);

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.print("<table>");

    for (String k : keys) {
      String value = get(k);
      pw.println("<tr><th>" + k + "</th>");
      pw.print("<td>");
      pw.print("<div class=\"" + k + "\">");
      pw.print(value);
      pw.print("</div>");
      pw.println("</td>");
      pw.println("</tr>");
    }

    pw.print("</table>");

    pw.flush();

    return sw.getBuffer().toString();
  }

  /**
   * If a logical node does not cleanly close after the specified amount of
   * time, it is is interrupted and should exit in error state. This can occur
   * because of some blocking sources, sinks, or decorators.
   */
  public long getNodeCloseTimeout() {
    return getLong(NODE_CLOSE_TIMEOUT, 30000);
  }
}
