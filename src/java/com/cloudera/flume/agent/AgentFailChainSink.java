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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.master.availability.FailoverChainManager;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.NetUtils;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This build "auto agents" (automatically generated fail over chains) at
 * different reliability levels
 */
public class AgentFailChainSink extends EventSink.Base {
  final static Logger LOG = Logger.getLogger(AgentFailChainSink.class.getName());
  final EventSink snk;

  public enum RELIABILITY {
    E2E, DFO, BE
  };

  public AgentFailChainSink(RELIABILITY rel, String... hosts)
      throws FlumeSpecException {
    this(new Context(), rel, hosts);
  }

  public AgentFailChainSink(Context context, RELIABILITY rel, String... hosts)
      throws FlumeSpecException {

    int defaultPort = FlumeConfiguration.get().getCollectorPort();
    List<String> thriftlist = thriftifyArgs(defaultPort, Arrays.asList(hosts));

    switch (rel) {
    case E2E: {
      String chains = AgentFailChainSink.genE2EChain(thriftlist
          .toArray(new String[0]));
      LOG.info("Setting failover chain to  " + chains);
      snk = new CompositeSink(context, chains);

      break;
    }
    case DFO: {
      String chains = AgentFailChainSink.genDfoChain(thriftlist
          .toArray(new String[0]));
      LOG.info("Setting failover chain to  " + chains);
      snk = new CompositeSink(context, chains);
      break;
    }
    case BE: {
      String chains = AgentFailChainSink.genBestEffortChain(thriftlist
          .toArray(new String[0]));
      LOG.info("Setting failover chain to  " + chains);
      snk = new CompositeSink(context, chains);
      break;
    }
    default: {
      throw new FlumeSpecException("Unknown relability " + rel);
    }
    }
  }

  @Override
  public void open() throws IOException {
    snk.open();
  }

  @Override
  public void close() throws IOException {
    snk.close();
  }

  @Override
  public void append(Event e) throws IOException {
    snk.append(e);
    super.append(e);
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    snk.getReports(namePrefix + getName() + ".", reports);
  }

  /**
   * Generates a best effort chain (will drop on failures)
   * 
   * TODO (jon) this needs to be live tested.
   */
  public static String genBestEffortChain(String... chain) {
    String body = "{ lazyOpen => { stubbornAppend => %s } }  ";

    // what happens when there are no collectors?
    String spec = FailoverChainManager.genAvailableSinkSpec(body, Arrays
        .asList(chain));
    LOG.info("Setting best effort failover chain to  " + spec);
    return spec;
  }

  /**
   * Generates a e2e acking chain. Writes to WAL then tries to send to failovers
   * 
   * TODO (jon) this needs to be live tested.
   */
  public static String genE2EChain(String... chain) {
    String body = " %s ";

    // what happens when there are no collectors?
    String spec = FailoverChainManager.genAvailableSinkSpec(body, Arrays
        .asList(chain));
    spec = "{ ackedWriteAhead => { stubbornAppend => { insistentOpen => "
        + spec + " } } }";
    LOG.info("Setting e2e failover chain to  " + spec);
    return spec;
  }

  /**
   * Generates a dfo chain. Tries best effort and then writes to dfo log if
   * failed. Tries to resend best effort.
   * 
   * TODO (jon) this needs to be live tested.
   */
  public static String genDfoChain(String... chain) {
    StringBuilder sb = new StringBuilder();
    String primaries = genBestEffortChain(chain);
    sb.append("let primary := " + primaries);
    String body = "< primary ? {diskFailover => { insistentOpen =>  primary} } >";

    LOG.info("Setting dfo failover chain to  " + body);
    sb.append(" in ");
    sb.append(body);

    return sb.toString();
  }

  /**
   * take a list of collectors and convert into a list of thrift sinks
   */
  public static List<String> thriftifyArgs(int defaultPort, List<String> list) {
    ArrayList<String> thriftified = new ArrayList<String>();

    if (list == null || list.size() == 0) {
      String sink = String.format("tsink(\"%s\",%d)", FlumeConfiguration.get()
          .getCollectorHost(), FlumeConfiguration.get().getCollectorPort());
      thriftified.add(sink);
      return thriftified;
    }

    for (String socket : list) {
      Pair<String, Integer> sock = NetUtils.parseHostPortPair(socket,
          defaultPort);
      String collector = sock.getLeft();
      int port = sock.getRight();
      // This needs to be a physical address/node, not a logical node.
      String sink = String.format("tsink(\"%s\",%d)", collector, port);
      thriftified.add(sink);
    }
    return thriftified;
  }

  /**
   * This builder creates a end to end acking, writeahead logging sink. This has
   * the recovery mechanism of the WAL and a retry mechanism that resends events
   * if user specified amount of time has passed without receiving an ack.
   * 
   * The first argument is required, but all failovers are optional.
   * 
   * agentE2EChain("machine1[:port]" [, "machine2[:port]" [,...]])
   */
  public static SinkBuilder e2eBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions
            .checkArgument(argv.length >= 1,
                "usage: agentE2EChain(\"machine1[:port]\" [, \"machine2[:port]\" [,...]])");
        try {
          return new AgentFailChainSink(RELIABILITY.E2E, argv);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  /**
   * This builder creates a disk failover logging sink. If a detectable error
   * occurs, it falls back to writing to local disk and then resending data from
   * disk.
   * 
   * The first argument is required, but all failovers are optional.
   * 
   * agentDFOChain("machine:port" [, port [,...]])
   * 
   * TODO(jon) Need to reimplement/check this to make sure it still works in
   * light of the changes to disk log management mechanisms.
   */
  public static SinkBuilder dfoBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions
            .checkArgument(argv.length >= 1,
                "usage: agentDFOChain(\"machine1[:port]\" [, \"machine2[:port]\" [,...]])");
        try {
          return new AgentFailChainSink(RELIABILITY.DFO, argv);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  /**
   * This builder creates a best effort logging sink. If a detectable error
   * occurs, it moves on without trying to recover or retry.
   * 
   * The first argument is required, but all failovers are optional.
   * 
   * agentBEChain("machine1[:port]" [, "machine2[:port]" [,...]])
   */
  public static SinkBuilder beBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions
            .checkArgument(argv.length >= 1,
                "usage: agentBEChain(\"machine1[:port]\" [, \"machine2[:port]\" [,...]])");
        try {
          return new AgentFailChainSink(RELIABILITY.BE, argv);
        } catch (FlumeSpecException e) {
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  @Override
  public String getName() {
    return "FailchainAgent";
  }

}
