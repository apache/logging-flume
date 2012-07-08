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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.Clock;
import com.google.common.base.Preconditions;

/**
 * This class handles ack checking against the master.
 * 
 * Here's how acks work. Agents attach extra checksuming/hash data to messages.
 * when these are received by the collector, and the values match, an ack for
 * the group's label is sent to the master. Meanwhile, the agent has a list of
 * acks it is expecting and periodically checks the master to see which have
 * arrived.
 * 
 * Potential problems: the master will end up maintaining alot of acks. possible
 * solutions: 0 ) punt. (which is what this first cut will do) 1) Force an
 * order. This ends up working alot like sequence numbers then but bounds state
 * 2) Time out values acks. Worst case the acks get missed and the data gets
 * sent again. 3) Send notification that ack is received. This allows memory to
 * be reclaimed proactively. 4) Stale acks get consolidated into larger ack
 * groups. (latency is less important if we have alot of stale stuff, throughput
 * more important)
 * 
 * TODO (jon) Rename to SenderAckManager
 * 
 * TODO (jon) decouple acks from the WAL
 */
public class WALAckManager implements Reportable {
  private static final String A_RETRANSMIT_TIMEOUT = "retransmitTimeout";
  private static final String A_PENDING_ACK_INFO = "pendingAckInfo";

  static final Logger LOG = LoggerFactory.getLogger(WALAckManager.class);

  // a pending set of acks
  final ConcurrentHashMap<String, Long> pending = new ConcurrentHashMap<String, Long>();
  MasterRPC client;
  final AckListener queuer = new PendingAckQueuer();
  final AckListener listener;
  final long retransmitTime;

  WALAckManager(MasterRPC c, AckListener listener, long ackRetransmit) {
    Preconditions.checkNotNull(c);
    Preconditions.checkNotNull(listener);
    this.client = c;
    this.listener = listener;
    this.retransmitTime = ackRetransmit;
  }

  /**
   * This is a handler for the ChecksumAckInjector to put tags and checksums it
   * expects. before it can delete things.
   */
  class PendingAckQueuer extends AckListener.Empty {
    @Override
    public void end(String group) throws IOException {
      long now = Clock.unixTime();
      LOG.info("Ack for " + group + " is queued to be checked");
      synchronized (pending) {
        pending.put(group, now);
      }
    }
  };

  public AckListener getAgentAckQueuer() {
    return queuer;
  }

  /**
   * This contacts the master to find if any of the pending acks are completed,
   */
  synchronized public void checkAcks() {
    LOG.debug("agent acks waiting for master: " + pending);

    // TODO (make this a batch operation with only one RPC call)
    List<String> done = new ArrayList<String>();
    for (String k : pending.keySet()) {
      try {
        boolean acked = client.checkAck(k);
        if (acked) {
          done.add(k);
        }
      } catch (IOException e) {
        // TODO (jon) there is a potential inconsistency here if master comms
        // fail (but this is recovered when retry happens).
        LOG.error("Master connection exception", e);
      } catch (RuntimeException re) {
        LOG.warn("check ack was in a illegal state", re);
      }
    }

    for (String k : done) {
      try {
        listener.end(k);
        pending.remove(k);
        LOG.debug("removed ack tag from agent's ack queue: " + k);
      } catch (IOException e) {
        LOG.error("problem notifying agent pending ack queue", e);
      } catch (RuntimeException re) {
        LOG.warn("check ack: runtime exception", re);
      }
    }

  }

  /**
   * This checks the pending table to see if any acks have been idle for too
   * long and need to be retried.
   */
  synchronized void checkRetry() {
    long now = Clock.unixTime();
    List<String> retried = new ArrayList<String>();
    for (Entry<String, Long> ack : pending.entrySet()) {
      long delta = now - ack.getValue();
      if (delta > retransmitTime) {
        // retransmit.. enqueue to retransmit.... move it back to agent dir..
        // (lame but good enough for now)
        try {
          LOG.info("Retransmitting " + ack.getKey() + " after being stale for "
              + delta + "ms");
          listener.expired(ack.getKey());
          retried.add(ack.getKey());
        } catch (IOException e) {
          LOG.error("problem notifying agent pending ack queue", e);
        }
      }
    }
    // update the time of entries to retry
    for (String key : retried) {
      pending.put(key, now);
    }
  }

  synchronized void forceRetry() {
    long now = Clock.unixTime();
    List<String> retried = new ArrayList<String>();
    for (Entry<String, Long> ack : pending.entrySet()) {
      // retransmit.. enqueue to retransmit.... move it back to agent dir..
      // (lame but good enough for now)
      try {
        LOG.info("Retransmitting " + ack.getKey());
        listener.expired(ack.getKey());
        retried.add(ack.getKey());
      } catch (IOException e) {
        LOG.error("problem notifying agent pending ack queue", e);
      }

    }
    // update the time of entries to retry
    for (String key : retried) {
      pending.put(key, now);
    }
  }

  @Override
  public String getName() {
    return "AgentWALAckManager";
  }

  @Override
  synchronized public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    Attributes.setLong(rpt, A_RETRANSMIT_TIMEOUT, retransmitTime);
    StringBuilder pendingAcks = new StringBuilder();
    for (Map.Entry<String, Long> e : pending.entrySet()) {
      pendingAcks.append(e.getKey());
      pendingAcks.append(":");
      pendingAcks.append(new Date(e.getValue()).toString());
      pendingAcks.append(", ");
    }
    Attributes.setString(rpt, A_PENDING_ACK_INFO, pendingAcks.toString());
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

  public Set<String> getPendingAckTags() {
    return Collections.unmodifiableSet(pending.keySet());
  }
}
