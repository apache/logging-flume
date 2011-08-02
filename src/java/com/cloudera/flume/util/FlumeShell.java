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
package com.cloudera.flume.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jline.Completor;
import jline.ConsoleReader;

import org.antlr.runtime.RecognitionException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.VersionInfo;
import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;

import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.reporter.server.FlumeReport;
import com.cloudera.flume.reporter.server.FlumeReportServer;
import com.cloudera.flume.shell.CommandBuilder;
import com.cloudera.util.CheckJavaVersion;

/**
 * The FlumeShell is a command-line-interface for a Flume master.
 * 
 * TODO (jon) move to com.cloudera.flume.shell
 */
public class FlumeShell {
  private static final Logger LOG = Logger.getLogger(FlumeShell.class);
  // Sort on command name
  protected static final Map<String, CommandDescription> commandMap = new TreeMap<String, CommandDescription>();
  protected boolean printPrompt = true;

  protected boolean done = false;
  protected long lastCmdId = 0;

  static final int POLL_DELAY_MS = 500;

  /**
   * Controls whether command prompt is shown
   */
  public FlumeShell(boolean printPrompt) {
    this.printPrompt = printPrompt;
  }

  /**
   * Default constructor - command prompt on
   */
  public FlumeShell() {
    this(true);
  }

  /**
   * Super simple class to aggregate some command metadata.
   */
  static class CommandDescription {
    final String usage;
    final boolean needsCnxn;
    final int arity;

    CommandDescription(String usage, boolean needsCnxn, int minArity) {
      this.usage = usage;
      this.needsCnxn = needsCnxn;
      this.arity = minArity;
    }
  }

  static {
    commandMap.put("connect", new CommandDescription("host[:adminport="
        + FlumeConfiguration.get().getConfigAdminPort() + "[:reportport="
        + FlumeConfiguration.get().getConfigReportPort() + "]]", false, 1));
    commandMap.put("getnodestatus", new CommandDescription("", true, 0));
    commandMap.put("quit", new CommandDescription("", false, 0));
    commandMap.put("getconfigs", new CommandDescription("", true, 0));
    commandMap.put("getmappings", new CommandDescription("[physical node]", true, 0));
    commandMap.put("source", new CommandDescription(
        "load a file and execute flume shell commands in it", false, 1));

    commandMap.put("help", new CommandDescription("", false, 0));

    // TODO (jon) hide this or format it better in help
    commandMap.put("exec", new CommandDescription(
        "   (requires a command and arguments)", true, 1));
    commandMap.put("submit", new CommandDescription(
        "   (requires a command and arguments)", true, 1));
    commandMap.put("wait", new CommandDescription(
        " [maxmillis (0==infinite) [, cmdid]]", true, 0));
    commandMap.put("waitForNodesDone", new CommandDescription(
        " [maxmillis (0==infinite) [, node[, ...]]]", true, 2));
    commandMap.put("waitForNodesActive", new CommandDescription(
        " [maxmillis (0==infinite) [, node[, ...]]]", true, 2));

    // These actually work well and autocomplete the way we want!
    commandMap.put("exec config", new CommandDescription(
        "node 'source' 'sink'", true, 1));
    commandMap.put("exec multiconfig",
        new CommandDescription("'spec'", true, 1));
    commandMap.put("exec refresh", new CommandDescription("'spec'", true, 1));
    commandMap.put("exec refreshAll", new CommandDescription("", true, 1));
    commandMap.put("exec noop", new CommandDescription(
        "[delaymillis (no arg means no wait)]", true, 1));
    commandMap.put("exec spawn", new CommandDescription(
        "physicalnode logicalnode (synonym for exec map. deprecated.)", true, 3));
    commandMap.put("exec map", new CommandDescription(
        "physicalnode logicalnode", true, 3));
    commandMap.put("exec decommission", new CommandDescription("logicalnode",
        true, 2));
    commandMap.put("exec unmap", new CommandDescription(
        "physicalnode logicalnode", true, 3));
    commandMap.put("exec unmapAll", new CommandDescription("", true, 1));

    // These actually work well and autocomplete the way we want!
    commandMap.put("submit config", new CommandDescription(
        "node 'source' 'sink'", true, 1));
    commandMap.put("submit multiconfig", new CommandDescription("'spec'", true,
        1));
    commandMap.put("submit refresh", new CommandDescription("'spec'", true, 1));
    commandMap.put("submit refreshAll", new CommandDescription("", true, 1));
    commandMap.put("submit noop", new CommandDescription("", true, 1));
    commandMap.put("submit spawn", new CommandDescription(
        "physicalnode logicalnode (synonym for submit map. deprecated.)", true, 3));
    commandMap.put("submit map", new CommandDescription(
        "physicalnode logicalnode", true, 3));
    commandMap.put("submit decommission", new CommandDescription("logicalnode",
        true, 2));
    commandMap.put("submit unmap", new CommandDescription(
        "physicalnode logicalnode", true, 3));
    commandMap.put("submit unmapAll", new CommandDescription("", true, 1));
    commandMap.put("getreports", new CommandDescription("", true, 0));

  }

  protected AdminRPC client = null;
  protected FlumeReportServer.Client reportClient = null;
  public static final long CMD_WAIT_TIME_MS = 10 * 1000;

  protected static class FlumeCompletor implements Completor {

    @SuppressWarnings("unchecked")
    // :(
    @Override
    public int complete(String buffer, int cursor, List candidates) {
      for (String s : commandMap.keySet()) {
        if (s.startsWith(buffer)) {
          candidates.add(s);
        }
      }
      return 0;
    }
  }

  protected static class ShellCommand {
    protected String command;
    final protected List<String> args;

    public String getCommand() {
      return command;
    }

    public List<String> getArgs() {
      return Collections.unmodifiableList(args);
    }

    public ShellCommand(Command c) {
      this.command = c.getCommand();
      this.args = Arrays.asList(c.getArgs());
    }

    public ShellCommand(String line) throws RecognitionException {
      this(CommandBuilder.parseLine(line));
    }

  }

  protected boolean connected = false;
  protected String curhost = "";
  protected int curAPort = 0;
  protected int curRPort = 0;

  protected void disconnect() {
    System.out.println("Disconnected!");
    connected = false;
  }

  protected void printHello() {
    System.out.println("==================================================");
    System.out.println("FlumeShell v" + VersionInfo.getVersion());
    System.out.println("Copyright (c) Cloudera 2010, All Rights Reserved");
    System.out.println("==================================================");
    System.out.println("Type a command to execute (hint: many commands");
    System.out.println("only work when you are connected to a master node)");
    System.out.println("");
    System.out.println("You may connect to a master node by typing: ");
    System.out.println("    connect host[:adminport="
        + FlumeConfiguration.get().getConfigAdminPort() + "[:reportport="
        + FlumeConfiguration.get().getConfigReportPort() + "]]");
    System.out.println("");
  }

  protected void printHelp() {
    System.out.println("I know about these commands:");
    System.out.println("");
    for (Entry<String, CommandDescription> e : commandMap.entrySet()) {
      System.out.println("    " + e.getKey() + " " + e.getValue().usage);
    }

  }

  /**
   * This waits for a command cmdid for at most maxmillis to reach SUCCESS or
   * FAIL state. Returns -1 if timed out, returns 0 on sucess.
   */
  protected long pollWait(long cmdid, long maxmillis) throws IOException,
      InterruptedException {

    if (!client.hasCmdId(cmdid)) {
      System.out.println("Command id " + cmdid + " does not exist");
      return -1;
    }

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < maxmillis) {

      if (client.isFailure(cmdid)) {
        System.out.println("Command failed");
        // TODO (jon) get the commands history message and display it.
        return -1;
      }
      if (client.isSuccess(cmdid)) {
        System.out.println("Command succeeded");

        // TODO (jon) get the commands history message and display it.
        return 0;
      }
      Thread.sleep(POLL_DELAY_MS);
    }
    System.out.println("Command timed out");
    return -1;

  }

  boolean isDone(StatusManager.NodeStatus status) {
    StatusManager.NodeState state = status.state;
    switch (state) {
    case IDLE:
    case ERROR:
    case LOST:
      // if at version 0, do not return true for isDone. (nothing has happened!)
      return status.version != 0;
    case ACTIVE:
    case CONFIGURING:
    case HELLO:
    default:
      return false;
    }
  }

  /**
   * This waits for at most maxmillis for all nodes to reach IDLE or ERROR state
   */
  protected long pollNodesDone(List<String> nodes, long maxmillis)
      throws InterruptedException {
    long start = System.currentTimeMillis();

    while (System.currentTimeMillis() - start < maxmillis) {
      Map<String, StatusManager.NodeStatus> nodemap;
      try {
        nodemap = client.getNodeStatuses();
      } catch (IOException e) {
        LOG.debug("Disconnected!", e);
        disconnect();
        return -1;
      }

      boolean busy = false;
      for (String n : nodes) {
        StatusManager.NodeStatus stat = nodemap.get(n);
        if (stat == null) {
          busy = true;
          break;
        }
        if (!isDone(nodemap.get(n))) {
          busy = true;
          break;
        }
      }

      if (!busy) {
        System.out.println("Nodes are done (in IDLE or in ERROR state)");
        return 0;
      }

      // busy
      Thread.sleep(500);
    }
    System.out.println("Command timed out");
    disconnect();
    return -1;
  }

  boolean isActive(StatusManager.NodeStatus status) {
    StatusManager.NodeState state = status.state;
    switch (state) {
    case ACTIVE:
    case CONFIGURING:
      return status.version != 0;
    case IDLE:
    case ERROR:
    case HELLO:
    case LOST:
    default:
      return false;
    }
  }

  /**
   * This checks to see if all the nodes in the specified list are in ACTIVE or
   * CONFIGURING state. This is fragile and temporary and should not be publicly
   * user exposed.
   * 
   * Really what I want is a mechanism to track state transitions.
   */
  protected long pollNodesActive(List<String> nodes, long maxmillis)
      throws InterruptedException {
    long start = System.currentTimeMillis();

    while (System.currentTimeMillis() - start < maxmillis) {
      Map<String, StatusManager.NodeStatus> nodemap;
      try {
        nodemap = client.getNodeStatuses();
      } catch (IOException e) {
        LOG.debug("Disconnected! " + e.getMessage(), e);
        System.out.println("Disconnected! " + e.getMessage());
        disconnect();
        return -1;
      }

      // this isn't completely reliable yet -- it should be used with care.
      boolean ready = true;
      for (String n : nodes) {
        StatusManager.NodeStatus stat = nodemap.get(n);
        if (stat == null) {
          ready = false;
          break;
        }

        if (!isActive(stat)) {
          ready = false;
          break;
        }
      }

      if (ready) {
        System.out.println("Nodes are active (in ACTIVE or CONFIGURING state)");
        return 0;
      }

      // busy
      Thread.sleep(500);
    }
    System.out.println("Command timed out");
    disconnect();
    return -1;
  }

  /**
   * Exec the specified filename. This command is called "source" like in bash/c
   * shells
   */
  long execFile(String filename) {
    FileReader f = null;
    try {
      f = new FileReader(filename);
    } catch (IOException e) {
      System.out.println("unable to source file " + filename + ": "
          + e.getMessage());
      return -1;
    }

    BufferedReader in = new BufferedReader(f);
    String str;
    long lastret = 0;
    long lineno = 0;
    try {
      while ((str = in.readLine()) != null) {
        lineno++;
        lastret = executeLine(str);
      }
      return lastret;

    } catch (IOException ioe) {
      System.out.println("Problem reading line " + filename + ":" + lineno
          + " : " + ioe.getMessage());
      return -1;
    } finally {
      try {
        in.close();
        f.close();
      } catch (IOException e) {
        LOG.error("Unable to close " + f);
      }
    }
  }

  private static int parsePort(String inp, int defaultPort) {
    int port = -1;
    try {
      port = Integer.parseInt(inp);
    } catch (NumberFormatException nfe) {
      System.out.println("Cannot parse port number '" + inp
          + "', defaulting to " + defaultPort);
      return defaultPort;
    }

    if (port < 0 || port > (0xffff)) {
      System.out.println("Port number out of range: " + port
          + ", defaulting to " + defaultPort);
      return defaultPort;
    }
    return port;
  }

  private static int parseAdminPort(String arg) {
    // determine the admin port
    int aPortDefault = FlumeConfiguration.get().getConfigAdminPort();
    int aPort;
    if (arg != null) {
      aPort = parsePort(arg, aPortDefault);
    } else {
      aPort = aPortDefault;
      System.out.println("Using default admin port: " + aPort);
    }
    return aPort;
  }

  private static int parseReportPort(String arg) {
    // determine the report server port
    int rPortDefault = FlumeConfiguration.get().getConfigReportPort();
    int rPort;
    if (arg != null) {
      rPort = parsePort(arg, rPortDefault);
    } else {
      rPort = rPortDefault;
      System.out.println("Using default report port: " + rPort);
    }
    return rPort;
  }

  /**
   * This either returns 0 for success, a value <0 for failure, and any return
   * >0 is a command id received from the master.
   * 
   * Unexpected failures that go over the network should disconnect before
   * returning. (This is useful to "nullify" other commands if an error has
   * occurred)
   */
  protected long execCommand(ShellCommand cmd) {
    if (!commandMap.containsKey(cmd.getCommand())) {
      System.out.println("Unknown command");
      return -1;
    }

    CommandDescription cd = commandMap.get(cmd.getCommand());

    // Run a few guard checks to see if we can execute command
    if (cmd.getArgs().size() < cd.arity) {
      System.out.println("Must supply at least " + cd.arity + " arguments");
      return -1;
    }

    if (cd.needsCnxn && !connected) {
      System.out.println("Not connected");
      return -1;
    }

    // Exhaustive exit the shell.
    if (cmd.getCommand().equals("quit")) {
      disconnect();
      done = true;
      return 0;
    }

    // 'source' or exec the lines from a specified file.

    if (cmd.getCommand().equals("source")) {
      return execFile(cmd.getArgs().get(0));
    }

    if (cmd.getCommand().equals("getnodestatus")) {
      Map<String, StatusManager.NodeStatus> nodemap;
      try {
        nodemap = client.getNodeStatuses();
      } catch (IOException e) {
        LOG.debug("Disconnected!", e);
        disconnect();
        return -1;
      }
      System.out.println("Master knows about " + nodemap.size() + " nodes");

      for (Entry<String, StatusManager.NodeStatus> e : nodemap.entrySet()) {
        System.out.println("\t" + e.getKey() + " --> "
            + e.getValue().state.toString());
      }
      return 0;
    }

    if (cmd.getCommand().equals("getreports")) {
      Map<String, FlumeReport> reports;
      try {
        reports = reportClient.getAllReports();
      } catch (TException e) {
        LOG.debug("Disconnected!", e);
        disconnect();
        return -1;
      }
      System.out.println("Master knows about " + reports.size() + " reports");

      for (Entry<String, FlumeReport> e : reports.entrySet()) {
        System.out.println("\t" + e.getKey() + " --> "
            + e.getValue().toString());
      }
      return 0;
    }

    if (cmd.getCommand().equals("getmappings")) {
      Map<String, List<String>> mappings;
      String physicalNode = null;
      String forPhysicalMessage = "";

      if (cmd.args.size() > 0) {
        physicalNode = cmd.args.get(0);
        forPhysicalMessage = " for physical node " + physicalNode;
      }

      try {
        mappings = client.getMappings(physicalNode);
      } catch (IOException e) {
        LOG.debug("Disconnected!", e);
        disconnect();
        return -1;
      }

      String header = String.format("%s\n\n%-30s --> %s\n",
          "Master has the following mappings" + forPhysicalMessage,
          "Physical Node",
          "Logical Node(s)"
      );

      if (mappings.size() > 0) {
        System.out.println(header);

        for (Entry<String, List<String>> entry : mappings.entrySet()) {
          System.out.println(String.format("%-30s --> %s", entry.getKey(),
              entry.getValue()));
        }
      } else {
        System.out.println("No physical / logic node mappings" + forPhysicalMessage + ". Use spawn to map a logical node to a physical node.");
      }

      return 0;
    }

    // Waits until the list of specified nodes are either in IDLE or in ERROR
    // state
    if (cmd.getCommand().equals("waitForNodesDone")) {
      Long maxmillis = Long.parseLong(cmd.getArgs().get(0));
      List<String> nodes = cmd.getArgs().subList(1, cmd.getArgs().size());
      try {
        maxmillis = (maxmillis <= 0) ? Long.MAX_VALUE : maxmillis;
        System.out.println("Waiting for " + maxmillis + " ms for nodes "
            + nodes + " to be IDLE/ERROR/LOST");
        pollNodesDone(nodes, maxmillis);
      } catch (InterruptedException e) {
        System.out.println("Interrupted during command processing");
        LOG.debug("Interrupted!", e);
        return -1;
      }
    }

    // Waits until the list of specified nodes are either in ACTIVE or
    // CONFIGURING state
    if (cmd.getCommand().equals("waitForNodesActive")) {
      Long maxmillis = Long.parseLong(cmd.getArgs().get(0));
      List<String> nodes = cmd.getArgs().subList(1, cmd.getArgs().size());
      try {
        maxmillis = (maxmillis <= 0) ? Long.MAX_VALUE : maxmillis;
        System.out.println("Waiting for " + maxmillis + " ms for nodes "
            + nodes + " to be ACTIVE");
        pollNodesActive(nodes, maxmillis);
      } catch (InterruptedException e) {
        System.out.println("Interrupted during command processing");
        LOG.debug("Interrupted!", e);
        return -1;
      }
    }

    if (cmd.getCommand().equals("connect")) {
      List<String> servers = new ArrayList<String>(Arrays.asList(cmd.getArgs()
          .get(0).split(",")));
      for (String s : servers) {
        String[] parts = s.split(":");
        try {
          // determine the admin port
          int aPort = parseAdminPort(parts.length >= 2 ? parts[1] : null);
          // determine the report server port
          int rPort = parseReportPort(parts.length >= 3 ? parts[2] : null);
          try {
            connect(parts[0], aPort, rPort);
          } catch (IOException e) {
            System.out.println("Connection to " + s + " failed");
            LOG.debug("Connection to " + s + " failed", e);
          }
          return 0;
        } catch (TTransportException t) {
          System.out.println("Connection to " + s + " failed");
          LOG.debug("Connection to " + s + " failed", t);
        }
        return -1;
      }
      return 0;
    }

    /*
     * Submit sends a command to the master and returns its cmdid. This allows
     * for asynchronous issuing of commands.
     */
    if (cmd.getCommand().equals("submit")) {
      try {
        List<String> args = new ArrayList<String>();
        if (cmd.getArgs().size() > 1) {
          args = cmd.getArgs().subList(1, cmd.getArgs().size());
        }
        long cmdid = client.submit(new Command(cmd.getArgs().get(0),
            (String[]) args.toArray(new String[args.size()])));
        lastCmdId = cmdid;
        // Do not change this, other programs will likely depend on this.
        System.out.println("[id: " + cmdid + "] Submitted command : "
            + cmd.getArgs().get(0));
        return cmdid;
      } catch (IOException e) {
        disconnect();
        LOG.debug("config failed due to transport error", e);
        return -1;
      }
    }

    /*
     * Wait waits for the specified cmdid to be done (success or error), for a
     * specified amount of time. If no cmdid is specified, it is assumed to be
     * the last command issued by the shell, or the last command in the master's
     * command queue.
     * 
     * 0 wait time means forever.
     */
    if (cmd.getCommand().equals("wait")) {
      long millis = CMD_WAIT_TIME_MS;
      long cmdid = lastCmdId;

      // args are time, and then cmdid.
      List<String> args = cmd.getArgs();
      if (args.size() >= 1) {
        long ms = Long.parseLong(args.get(0));
        if (ms < 0) {
          System.out.println("Wait time <0 is illegal");
          return -1;
        }
        // if 0, effectively wait forever.
        millis = (ms == 0) ? Long.MAX_VALUE : ms;
      }

      if (args.size() >= 2) {
        cmdid = Long.parseLong(args.get(1));
      }

      try {
        return pollWait(cmdid, millis);
      } catch (IOException e) {
        disconnect();
        LOG.debug("config failed due to transport error", e);
        return -1;
      } catch (InterruptedException e) {
        System.out.println("Interrupted during command processing");
        LOG.debug("Interrupted!", e);
        return -1;
      }
    }

    /*
     * Exec sends a command to the master and polls for a response. These are
     * the commands which cause the master to do some processing. We wait
     * synchronously for success or failure.
     * 
     * There is no schema checking for these commands - so it is possible to
     * send ill-formed commands and receive a failure notification with no idea
     * of what went wrong.
     */
    if (cmd.getCommand().equals("exec")) {
      try {
        List<String> args = new ArrayList<String>();
        if (cmd.getArgs().size() > 1) {
          args = cmd.getArgs().subList(1, cmd.getArgs().size());
        }
        long cmdid = client.submit(new Command(cmd.getArgs().get(0),
            (String[]) args.toArray(new String[args.size()])));
        System.out.println("[id: " + cmdid + "] Execing command : "
            + cmd.getArgs().get(0));
        lastCmdId = cmdid;

        return pollWait(cmdid, CMD_WAIT_TIME_MS);
      } catch (IOException e) {
        disconnect();
        LOG.debug("config failed due to transport error", e);
        return -1;
      } catch (InterruptedException e) {
        System.out.println("Interrupted during command processing");
        LOG.debug("Interrupted!", e);
        return -1;
      }
    }

    if (cmd.getCommand().equals("getconfigs")) {
      try {
        Map<String, FlumeConfigData> configs = client.getConfigs();

        if (configs.size() == 0) {
          System.out.println("Master has no logical node configurations.");
          return 0;
        }

        int maxnode = 0, maxsink = 0, maxsource = 0, maxflow = 0;
        for (Entry<String, FlumeConfigData> e : configs.entrySet()) {
          maxnode = java.lang.Math.max(maxnode, e.getKey().length());
          maxsink = java.lang.Math.max(maxsink, e.getValue().sinkConfig
              .length());
          maxsource = java.lang.Math.max(maxsource, e.getValue().sourceConfig
              .length());
          maxflow = java.lang.Math.max(maxflow, e.getValue().getFlowID()
              .length());
        }

        String title = String.format("%-" + maxnode + "s\t%-" + maxflow
            + "s\t%-" + maxsource + "s\t%-" + maxsink + "s", "NODE", "FLOW",
            "SOURCE", "SINK");
        System.out.println(title);

        for (Entry<String, FlumeConfigData> e : configs.entrySet()) {
          String line = String.format("%-" + maxnode + "s\t%-" + maxflow
              + "s\t%-" + maxsource + "s\t%-" + maxsink + "s", e.getKey(), e
              .getValue().getFlowID(), e.getValue().sourceConfig,
              e.getValue().sinkConfig);
          System.out.println(line);
        }
      } catch (IOException e) {
        disconnect();
        LOG.error("getconfigs failed due to transport error");
        return -1;
      }
      return 0;
    }

    if (cmd.getCommand().equals("help")) {
      printHelp();
      return 0;
    }

    return -1;
  }

  /**
   * Executes a command specified by a string
   * 
   * Made public to be testable.
   */
  public long executeLine(String line) {
    // do nothing if no line, empty line or comment
    if (line == null) {
      return 0;
    }

    // trim white space and and then check
    line = line.trim();
    if (line.equals("") || line.startsWith("#")) {
      return 0;
    }

    try {
      ShellCommand cmd = new ShellCommand(line);
      return execCommand(cmd);
    } catch (Exception e) {
      System.err.println("Failed to run command '" + line + "' due to "
          + e.getMessage());
      LOG.error("Failed to run command '" + line + "'");
      return -1;
    }
  }

  protected String getPrompt() {
    if (!printPrompt) {
      return "";
    }

    return "[flume "
        + (connected ? (curhost + ":" + curAPort + ":" + curRPort)
            : "(disconnected)") + "] ";
  }

  private FlumeReportServer.Client connectReportClient(String host, int port)
      throws TTransportException {
    TTransport masterTransport = new TSocket(host, port);
    TProtocol protocol = new TBinaryProtocol(masterTransport);
    masterTransport.open();
    return new FlumeReportServer.Client(protocol);
  }

  protected void connect(String host, int aPort, int rPort) throws IOException,
      TTransportException {
    connected = false;
    System.out.println("Connecting to Flume master " + host + ":" + aPort + ":"
        + rPort + "...");

    String rpcType = FlumeConfiguration.get().getMasterHeartbeatRPC();
    if (FlumeConfiguration.RPC_TYPE_AVRO.equals(rpcType)) {
      client = new AdminRPCAvro(host, aPort);
    } else if (FlumeConfiguration.RPC_TYPE_THRIFT.equals(rpcType)) {
      client = new AdminRPCThrift(host, aPort);
    } else {
      throw new IOException("No valid RPC framework specified in config");
    }

    // use default for now
    reportClient = connectReportClient(host, rPort);

    curhost = host;
    curAPort = aPort;
    curRPort = rPort;
    connected = true;
  }

  public void run() throws IOException, TTransportException {
    ConsoleReader cReader = new ConsoleReader();
    cReader.addCompletor(new FlumeCompletor());

    String line;
    while (!done && (line = cReader.readLine(getPrompt())) != null) {
      try {
        executeLine(line);
      } catch (RuntimeException r) {
        System.out.println("RuntimeException caught: " + r.getMessage());
      }
    }
  }

  /**
   * Args are optional - the first arg is the host:port for the master to
   * connect to
   */
  public static void main(String[] args) throws IOException,
      TTransportException {
    FlumeNode.logVersion(LOG, Level.DEBUG);
    FlumeNode.logEnvironment(LOG, Level.DEBUG);
    // Make sure the Java version is not older than 1.6
    if (!CheckJavaVersion.isVersionOk()) {
      LOG
          .error("Exiting because of an old Java version or Java version in bad format");
      System.exit(-1);
    }
    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("?", false, "Command line usage");
    options.addOption("c", true, "Connect to master:adminport:reportport");
    options.addOption("e", true, "Run a single command");
    options.addOption("s", true, "Run a FlumeShell script");
    options.addOption("q", false,
        "Run in quiet mode - only print command results");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeShell", options, true);
      System.exit(-1);
    }

    if (cmd.hasOption('?')) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("FlumeShell", options, true);
      System.exit(0);
    }

    boolean print = true;
    if (cmd.hasOption('q')) {
      print = false;
    }

    FlumeShell shell = new FlumeShell(print);

    if (cmd.hasOption("c")) {
      String[] addr = cmd.getOptionValue("c").split(":");

      // determine the admin port
      int aPort = parseAdminPort(addr.length >= 2 ? addr[1] : null);
      // determine the report server port
      int rPort = parseReportPort(addr.length >= 3 ? addr[2] : null);

      shell.connect(addr[0], aPort, rPort);
    }

    if (cmd.hasOption("e")) {
      long ret = shell.executeLine(cmd.getOptionValue("e"));
      // return error code if negative, otherwise return 0.
      System.exit(ret < 0 ? (int) ret : 0);
    }

    if (cmd.hasOption("s")) {
      FileReader f = null;
      try {
        f = new FileReader(cmd.getOptionValue('s'));
      } catch (IOException e) {
        System.err.println("Failed to open script: " + cmd.getOptionValue('s'));
        System.err.println("Exception was: " + e.getMessage());
        System.exit(1);
      }
      BufferedReader in = new BufferedReader(f);
      String str;
      while ((str = in.readLine()) != null) {
        shell.executeLine(str);
      }
      System.exit(0);
    }
    if (!cmd.hasOption('q')) {
      shell.printHello();
    }
    shell.run();
  }
}
