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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.util.FixedPeriodBackoff;
import com.cloudera.util.Pair;
import com.cloudera.util.ResultRetryable;
import com.cloudera.util.RetryHarness;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This wraps a SingleMasterRPC and provides failover from one master to another.
 */
public class MultiMasterRPC implements MasterRPC {
  final static Logger LOG = Logger.getLogger(MultiMasterRPC.class.getName());
  final protected int MAX_RETRIES;
  final protected int RETRY_PAUSE_MS;
  final String rpcProtocol;
  protected MasterRPC masterRPC;
  protected final List<Pair<String, Integer>> masterAddresses;
  
  // Index of next master to try - wraps round.
  protected int nextMaster = 0;
  protected String curHost;
  protected int curPort = 0;
  
  /**
   * Reads the set of master addresses from the configuration. If randomize is
   * set, it will shuffle the list. When a failure is detected, the entire set
   * of other masters will be tried maxRetries times, with a pause of
   * retryPauseMS between sweeps.
   */
  public MultiMasterRPC(FlumeConfiguration conf, boolean randomize,
      int maxRetries, int retryPauseMS) {
    masterAddresses = conf.getMasterHeartbeatServersList();
    if (randomize) {
      Collections.shuffle(masterAddresses);
    }
    Pair<String, Integer> masterAddr = 
      conf.getMasterHeartbeatServersList().get(0);
    this.MAX_RETRIES = maxRetries;
    this.RETRY_PAUSE_MS = retryPauseMS;
    this.rpcProtocol = conf.getMasterHeartbeatRPC();
  }
  
  /**
   * Reads the set of master addresses from the configuration. If randomize is
   * set, it will shuffle the list.
   */
  public MultiMasterRPC(FlumeConfiguration conf, boolean randomize) {
    this(conf, randomize, conf.getAgentMultimasterMaxRetries(), conf
        .getAgentMultimasterRetryBackoff());
  }
  
  /**
   * Will return null if not connected
   */
  public synchronized String getCurHost() {
    return curHost;
  }

  /**
   * Will return 0 if not connected
   */
  public synchronized int getCurPort() {
    return curPort;
  }

  protected synchronized MasterRPC findServer()
      throws IOException {
    List<String> failedMasters = new ArrayList<String>();
    for (int i = 0; i < masterAddresses.size(); ++i) {
      Pair<String, Integer> host = masterAddresses.get(nextMaster);
      try {
        // Next time we need to try the next master along
        nextMaster = (nextMaster + 1) % masterAddresses.size();

        // We don't know for sure what state the connection is in at this
        // point, so to be safe force a close.
        close();
        MasterRPC out = null;
        if (FlumeConfiguration.RPC_TYPE_THRIFT.equals(rpcProtocol)) {
          out = new ThriftMasterRPC(host.getLeft(), host.getRight());
        } else if (FlumeConfiguration.RPC_TYPE_AVRO.equals(rpcProtocol)) {
          out = new AvroMasterRPC(host.getLeft(), host.getRight());
        } else {
          LOG.error("No valid RPC protocl in configurations.");
          continue;
        }
        curHost = host.getLeft();
        curPort = host.getRight();
        this.masterRPC = out;
        return out;

      } catch (Exception e) {
        failedMasters.add(host.getLeft() + ":" + host.getRight());
        LOG.debug("Couldn't connect to master at " + host.getLeft() + ":"
            + host.getRight() + " because: " + e.getMessage());
      }
    }
    throw new IOException("Could not connect to any master nodes (tried "
        + masterAddresses.size() + ": " + failedMasters + ")");
  }

  protected synchronized MasterRPC ensureConnected()
      throws TTransportException, IOException {
    return (masterRPC != null) ? masterRPC : findServer();
  }

  public synchronized void close() {
    // multiple close is ok.
    if (this.masterRPC != null) {
      try {
        this.masterRPC.close();
      } catch (IOException e) {
        LOG.warn("Failed to close connection with RPC master" + curHost);
      }
    }
    curHost = null;
    curPort = 0;
  }

  /**
   * A word about the pattern used here. Each RPC call could fail. If this is
   * detected we want to fail over the another master server.
   * 
   * We use Retryables (not perfect, but good enough!) for this. Once a call
   * fails by throwing a TException, we try to find another server and fail the
   * current attempt. Each attempt to find another server goes in the worst case
   * around the list of servers once.
   * 
   * This pattern itself should repeat (otherwise if there are two consecutive
   * server failures, due to taking two or more offline, we'll see exceptions
   * propagated back to the caller). So we use a retry policy that retries every
   * 5s, up to 12 times. The idea is that if a node can't reach any masters for
   * 1 minute it's problematic. At this point an exception goes back to the
   * caller, and it's their responsibility to deal with the loss.
   * 
   */

  abstract class RPCRetryable<T> extends ResultRetryable<T> {
    /**
     * Implement RPC call here.
     */
    abstract public T doRPC() throws IOException;

    public boolean doTry() {
      /**
       * Getting the locking efficient here is difficult because of subtle race
       * conditions. Since all access to MasterRPC is synchronized, we can
       * afford to serialize access to this block.
       */
      synchronized (MultiMasterRPC.this) {
        try {
          result = doRPC();
          return true;
        } catch (Exception e) {
          /**
           * A subtle race condition - if two RPC calls have failed and fallen
           * through to here, both might try and call findServer and race on the
           * next good server. This is why we synchronize the whole enclosing
           * try block.
           */
          try {
            LOG.info("Connection to master lost due to " + e.getMessage()
                + ", looking for another...");
            LOG.debug(e.getMessage(), e);
            findServer();
          } catch (IOException e1) {
            LOG.error("Unable to find a master server", e1);
          }
        }
        return false;
      }
    }
  }

  public FlumeConfigData getConfig(final LogicalNode n) throws IOException {
    RPCRetryable<FlumeConfigData> retry = new RPCRetryable<FlumeConfigData>() {
      public FlumeConfigData doRPC() throws IOException {
        return masterRPC.getConfig(n);
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
      return retry.getResult();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * This checks for an ack with a given ackid at the master
   */
  public boolean checkAck(final String ackid) throws IOException {
    RPCRetryable<Boolean> retry = new RPCRetryable<Boolean>() {
      public Boolean doRPC() throws IOException {
        return masterRPC.checkAck(ackid);
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
      return retry.getResult();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  public List<String> getLogicalNodes(final String physicalNode) 
  throws IOException {
    RPCRetryable<List<String>> retry = new RPCRetryable<List<String>>() {
      public List<String> doRPC() throws IOException {
        return masterRPC.getLogicalNodes(physicalNode);
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
      return retry.getResult();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean heartbeat(final LogicalNode n) throws IOException {
    RPCRetryable<Boolean> retry = new RPCRetryable<Boolean>() {
      public Boolean doRPC() throws IOException {
        return masterRPC.heartbeat(n);
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
      return retry.getResult();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void acknowledge(final String group) throws IOException {
    RPCRetryable<Void> retry = new RPCRetryable<Void>() {
      public Void doRPC() throws IOException {
        masterRPC.acknowledge(group);
        return result; // Have to return something, but no-one will ever check
        // it
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void putReports(final Map<String, ReportEvent> reports)
      throws IOException {
    RPCRetryable<Void> retry = new RPCRetryable<Void>() {
      public Void doRPC() throws IOException {
        masterRPC.putReports(reports);
        return result;
      }
    };

    RetryHarness harness = new RetryHarness(retry, new FixedPeriodBackoff(
        RETRY_PAUSE_MS, MAX_RETRIES), true);
    try {
      harness.attempt();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public AckListener createAckListener() {
    return masterRPC.createAckListener();
  }
}
