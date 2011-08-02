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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This is the default agent sink in a agent/collector architecture.
 * 
 * This is purposely setup as a layer of indirection between the actual
 * implementation details to may user configuration simpler. It has a default
 * options that come from flume-*.xml configuration file.
 * 
 * For the easiest configuration path, all agents can use this as the default
 * sink. This only gives high level options to the user by hiding implementation
 * and mechanism details.
 * 
 * The actual implementation may change in the future (for example, thrift may
 * be replaced with avro, or we may have a ring of collectors to send to instead
 * of a single collector..) but user configurations would not need to change.
 * 
 * TODO (jon) replace with substitution instead of this sink.
 */
public class AgentSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(AgentSink.class);

  public enum ReliabilityMode {
    ENDTOEND, // this does writeahead along with end-to-end acks
    // TODO (jon) maybe do acks but no writeahead
    DISK_FAILOVER, // this is equivalent to scribe's failover mechanism
    BEST_EFFORT, // this is equivalent to syslog's best effort mechanism.
  };

  final EventSink sink;

  public AgentSink(String dsthost, int port, ReliabilityMode mode)
      throws FlumeSpecException {
    Preconditions.checkNotNull(dsthost);

    switch (mode) {
    case ENDTOEND: {
      String snk = String.format(
          "{ ackedWriteAhead => { stubbornAppend =>  { insistentOpen => "
              + "rpcSink(\"%s\", %d)} } }", dsthost, port);
      sink = FlumeBuilder.buildSink(new Context(), snk);
      break;
    }

    case DISK_FAILOVER: {

      // Move these options to the builder.
      FlumeConfiguration conf = FlumeConfiguration.get();
      long maxSingleBo = conf.getFailoverMaxSingleBackoff();
      long initialBo = conf.getFailoverInitialBackoff();
      long maxCumulativeBo = conf.getFailoverMaxCumulativeBackoff();
      String rpc = String.format("rpcSink(\"%s\", %d)", dsthost, port);

      String snk = String.format("< %s ? { diskFailover => { insistentAppend "
          + "=> { stubbornAppend => { insistentOpen(%d,%d,%d) => %s} } } } >",
          rpc, maxSingleBo, initialBo, maxCumulativeBo, rpc);
      sink = FlumeBuilder.buildSink(new Context(), snk);
      break;

    }

    case BEST_EFFORT: {
      String snk = String.format("< { insistentOpen => { stubbornAppend => "
          + "rpcSink(\"%s\", %d) } }  ? null>", dsthost, port);
      sink = FlumeBuilder.buildSink(new Context(), snk);
      break;
    }

    default:
      throw new FlumeSpecException("unexpected agent mode: " + mode + "!");
    }

  }

  @Override
  public void append(Event e) throws IOException {
    sink.append(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
    sink.close();
  }

  @Override
  public void open() throws IOException {
    sink.open();
  }

  /**
   * This builder creates a end to end acking, writeahead logging sink. This has
   * the recovery mechanism of the WAL and a retry mechanism that resends events
   * if user specified amount of time has passed without receiving an ack.
   * 
   * The arguments for this are optional. the first is the name of the collector
   * machine, and the second is a port
   * 
   * e2eWal[("machine" [, port ] )]
   */
  public static SinkBuilder e2eBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 2);
        FlumeConfiguration conf = FlumeConfiguration.get();
        String collector = conf.getCollectorHost();
        int port = conf.getCollectorPort();
        if (argv.length >= 1) {
          collector = argv[0];
        }

        if (argv.length >= 2) {
          port = Integer.parseInt(argv[1]);
        }

        try {
          return new AgentSink(collector, port, ReliabilityMode.ENDTOEND);
        } catch (FlumeSpecException e) {
          LOG.error("AgentSink spec error " + e, e);
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
   * The arguments for this are optional. The first is the name of the collector
   * machine, and the second is a port
   * 
   * dfo[("machine" [, port ] )]
   * 
   * TODO(jon) Need to reimplement/check this to make sure it still works in
   * light of the changes to disk log management mechanisms.
   */
  public static SinkBuilder dfoBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 2);
        FlumeConfiguration conf = FlumeConfiguration.get();
        String collector = conf.getCollectorHost();
        int port = conf.getCollectorPort();
        if (argv.length >= 1) {
          collector = argv[0];
        }

        if (argv.length >= 2) {
          port = Integer.parseInt(argv[1]);
        }

        try {
          return new AgentSink(collector, port, ReliabilityMode.DISK_FAILOVER);
        } catch (FlumeSpecException e) {
          LOG.error("AgentSink spec error " + e, e);
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  /**
   * This builder creates a best effort logging sink. If a detectable error
   * occurs, it moves on without trying to recover or retry.
   * 
   * The arguments for this are optional. The first is the name of the collector
   * machine, and the second is a port
   * 
   * be[("machine" [, port ] )]
   */
  public static SinkBuilder beBuilder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 2);
        FlumeConfiguration conf = FlumeConfiguration.get();
        String collector = conf.getCollectorHost();
        int port = conf.getCollectorPort();
        if (argv.length >= 1) {
          collector = argv[0];
        }

        if (argv.length >= 2) {
          port = Integer.parseInt(argv[1]);
        }

        try {
          return new AgentSink(collector, port, ReliabilityMode.BEST_EFFORT);
        } catch (FlumeSpecException e) {
          LOG.error("AgentSink spec error " + e, e);
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  @Override
  public String getName() {
    return "Agent";
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    sink.getReports(namePrefix + getName() + ".", reports);
  }
}
