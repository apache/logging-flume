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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.master.commands.CreateLogicalNodeForm;
import com.cloudera.flume.master.commands.DecommissionLogicalNodeForm;
import com.cloudera.flume.master.commands.PurgeAllCommand;
import com.cloudera.flume.master.commands.PurgeCommand;
import com.cloudera.flume.master.commands.RefreshAllCommand;
import com.cloudera.flume.master.commands.RefreshCommand;
import com.cloudera.flume.master.commands.SetChokeLimitForm;
import com.cloudera.flume.master.commands.UnconfigCommand;
import com.cloudera.flume.master.commands.UnmapLogicalNodeForm;
import com.cloudera.flume.master.commands.UpdateAllCommand;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.google.common.base.Preconditions;

/**
 * We want to serialize the order of configuration commands sent to the flume
 * configuration server. To do this we break up actions into Commands, and
 * submit a command to the command manager. The commands are queued into an
 * order and eventually they are exec'ed by the main execution thread.
 * 
 * This must be thread safe.
 * 
 * TODO (jon) this stores data in memory and will eventually exhaust memory.
 * need to add some cleanup persist old values after some threshold
 * (time/space).
 */
public class CommandManager implements Reportable {
  static final Logger LOG = LoggerFactory.getLogger(CommandManager.class);

  // queue of commands pending execution.
  final LinkedBlockingQueue<CommandStatus> queue = new LinkedBlockingQueue<CommandStatus>();

  // map from command string to executable command. Since we don no provide a
  // mechanism to add more commands, this is essentially
  // constant, and doesn't need guarding.
  final Map<String, Execable> cmds = new HashMap<String, Execable>();

  // This must be accessed in a thread safe way.
  final SortedMap<Long, CommandStatus> statuses = new TreeMap<Long, CommandStatus>();

  final AtomicLong curCommandId = new AtomicLong();

  static Execable noopExec = new Execable() {
    /**
     * Optional argument is a time to sleep in millis
     */
    public void exec(String[] args) throws MasterExecException {
      if (args.length == 1) {
        long delay = Long.parseLong(args[0]);
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          LOG.debug("Delay noop interrupted", e);
          throw new MasterExecException("Delay Noop Interrupted!", e);
        }
      }
    }
  };

  ExecThread execThread;
  final static Object[][] cmdArrays = { { "noop", noopExec },
      { "config", ConfigCommand.buildExecable() },
      { "multiconfig", MultiConfigCommand.buildExecable() },
      { "unconfig", UnconfigCommand.buildExecable() },
      { "refresh", RefreshCommand.buildExecable() },
      { "refreshAll", RefreshAllCommand.buildExecable() },
      { "purge", PurgeCommand.buildExecable() },
      { "purgeAll", PurgeAllCommand.buildExecable() },
      { "updateAll", UpdateAllCommand.buildExecable() },
      { "save", SaveConfigCommand.buildExecable() },
      { "load", LoadConfigCommand.buildExecable() },
      { "spawn", CreateLogicalNodeForm.buildExecable() },
      { "map", CreateLogicalNodeForm.buildExecable() },
      { "decommission", DecommissionLogicalNodeForm.buildExecable() },
      { "unmap", UnmapLogicalNodeForm.buildExecable() },
      { "unmapAll", UnmapLogicalNodeForm.buildUnmapAllExecable() },
      { "setChokeLimit", SetChokeLimitForm.buildExecable() }

  };

  public CommandManager() {
    this(cmdArrays);
  }

  /*
   * This is for testing
   */
  CommandManager(Object[][] cmdArray) {
    for (Object[] c : cmdArray) {
      cmds.put((String) c[0], (Execable) c[1]);
    }
  }

  /**
   * This adds a new command to the set commands that can be executed by the
   * CommandManager
   */
  public void addCommand(String cmd, Execable ex) {
    Preconditions.checkNotNull(cmd, "Command must not be null");
    Preconditions.checkNotNull(ex, "Execable must not be null");
    if (cmds.containsKey(cmd)) {
      LOG.warn("Command '" + cmd
          + "' previously existed and is being overwritten");
    }
    cmds.put(cmd, ex);
  }

  /**
   * Starts the exec thread (don't do this in a constructor)
   */
  synchronized public void start() {
    if (execThread != null) {
      LOG.error("Command Manager already started, not spawning another");
      return;
    }

    execThread = new ExecThread();
    execThread.start();
  }

  /**
   * Cleanly stops the command manager. This blocks until the execThreads are
   * complete.
   */
  synchronized public void stop() {
    execThread.shutdown();
    execThread = null;
  }

  public long submit(Command cmd) {
    Preconditions.checkNotNull(cmd, "No null commands allowed, use \"noop\"");
    LOG.info("Submitting command: " + cmd);
    long cmdId = curCommandId.getAndIncrement();
    CommandStatus cmdStat = CommandStatus.createCommandStatus(cmdId, cmd);
    synchronized (this) {
      // want this to be atomic
      statuses.put(cmdId, cmdStat);
      queue.add(cmdStat);
    }
    return cmdId;
  }

  public boolean isSuccess(long cmdid) {
    CommandStatus stat = null;
    synchronized (this) {
      stat = statuses.get(cmdid);
    }
    return stat != null && stat.isSuccess();
  }

  public boolean isFailure(long cmdid) {
    CommandStatus stat = null;
    synchronized (this) {
      stat = statuses.get(cmdid);
    }
    return stat != null && stat.isFailure();
  }

  /**
   * This layer should eat all exceptions, including runtime exceptions.
   */
  void handleCommand(CommandStatus cmd) {
    try {
      if (cmd == null) {
        return; // do nothing
      }

      cmd.toExecing("");
      exec(cmd.cmd); // if no exception, assumed to be successful
      cmd.toSucceeded("");
    } catch (MasterExecException e) {
      // Log and leave info about expected exceptions
      LOG.warn("During " + cmd + " : " + e.getMessage());
      cmd.toFailed(e.getMessage());
    } catch (Exception e) {
      // catches runtime and unexpected validation / illegal / preconditions
      // exceptions.
      LOG.error("Unexpected exception during " + cmd + " : " + e.getMessage(),
          e);
      cmd.toFailed(e.getMessage());
    }

  }

  class ExecThread extends Thread {
    volatile boolean done = false;

    CountDownLatch stopped = new CountDownLatch(1);

    ExecThread() {
      super("exec-thread");
    }

    @Override
    public void run() {
      try {
        while (!done) {
          // only have to worry about interrupted exns here.
          CommandStatus cmd = queue.poll(1000, TimeUnit.MILLISECONDS);
          handleCommand(cmd);
        }
      } catch (InterruptedException e) {
        LOG.error("Master exec thread interrupted!", e);
      } finally {
        stopped.countDown();
      }
    }

    public void shutdown() {
      done = true;
      try {
        stopped.await();
      } catch (InterruptedException e) {
        LOG.error("Shutdown of command manager was interrupted");
      }
    }

  }

  // package visible for testing.
  void exec(Command cmd) throws MasterExecException {
    Execable ex = cmds.get(cmd.getCommand());
    if (ex == null) {
      throw new MasterExecException("Don't know how to handle Command: '" + cmd
          + "'", null);
    }

    LOG.info("Executing command: " + cmd);
    try {
      ex.exec(cmd.getArgs());
    } catch (MasterExecException e) {
      throw e; // just rethrow
    } catch (IOException e) {
      throw new MasterExecException(e.getMessage(), e);
    }
    // other exceptions get handled at next layer.
  }

  @Override
  public String getName() {
    return "command manager";
  }

  public SortedMap<Long,CommandStatus> getStatuses() {
    return statuses;
  }

  /*
   * Return the Command status for the given submission id. If the id is not
   * present, it returns null.
   */
  public CommandStatus getStatus(long id) {
    return statuses.get(id);
  }

  // TODO (jon) convert to a regular report
  @Override
  public ReportEvent getMetrics() {

    StringBuilder html = new StringBuilder();
    html.append("<div class=\"CommandManager\">");
    html
        .append("<h2>Command history </h2>\n<table border=\"1\"><tr><th>id</th><th>State</th><th>command line</th><th>message</th></tr>\n");

    List<CommandStatus> values = null;

    synchronized (this) {
      values = new ArrayList<CommandStatus>(statuses.values());
    }

    for (CommandStatus stat : values) {
      html.append(" <tr>");

      html.append("  <td>");
      html.append(stat.getCmdID());
      html.append("</td>\n");

      html.append("  <td>");
      html.append(stat.getState());
      html.append("</td>\n");

      html.append("  <td>");
      html.append(stat.getCommand());
      html.append("</td>\n");

      html.append("  <td>");
      html.append(stat.getMessage());
      html.append("</td>\n");

      html.append(" </tr>\n");
    }

    html.append("</table>\n\n");
    html.append("</div>");

    return ReportEvent.createLegacyHtmlReport("", html.toString());
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }
}
