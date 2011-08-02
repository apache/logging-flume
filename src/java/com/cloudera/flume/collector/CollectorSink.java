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
package com.cloudera.flume.collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.MaskDecorator;
import com.cloudera.flume.handlers.debug.InsistentAppendDecorator;
import com.cloudera.flume.handlers.debug.InsistentOpenDecorator;
import com.cloudera.flume.handlers.debug.StubbornAppendSink;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CumulativeCappedExponentialBackoff;
import com.google.common.base.Preconditions;

/**
 * This collector sink is the high level specification a user would use.
 * 
 * Currently is is just a DFSEventSink that uses some default values from the
 * flume configuration file and is overridable by the user.
 * 
 * TODO (jon) this default output needs to be replaced with a combo of henry's
 * new tagged output writer and the custom format dfs writer..
 */
public class CollectorSink extends EventSink.Base {
  static final Logger LOG = LoggerFactory.getLogger(CollectorSink.class);

  final EventSink snk;
  AckAccumulator accum = new AckAccumulator();
  final AckListener ackDest;

  // This is a container for acks that should be ready for delivery when the
  // hdfs sink is closed/flushed
  Set<String> rollAckSet = new HashSet<String>();

  // References package exposed for testing
  final RollSink roller;

  CollectorSink(Context ctx, String path, String filename, long millis,
      AckListener ackDest) throws FlumeSpecException {
    this(ctx, path, filename, millis, new ProcessTagger(), 250, ackDest);
  }

  CollectorSink(Context ctx, final String logdir, final String filename,
      final long millis, final Tagger tagger, long checkmillis,
      AckListener ackDest) {
    this.ackDest = ackDest;
    this.roller = new RollSink(ctx, "collectorSink", new TimeTrigger(tagger,
        millis), checkmillis) {
      @Override
      public EventSink newSink(Context ctx) throws IOException {
        String tag = tagger.newTag();
        String path = logdir + Path.SEPARATOR_CHAR;
        EventSink dfs = new EscapedCustomDfsSink(path, filename + tag);
        return new RollDetectDeco(dfs, tag);
      }
    };

    long initMs = FlumeConfiguration.get().getInsistentOpenInitBackoff();
    long cumulativeMaxMs = FlumeConfiguration.get()
        .getFailoverMaxCumulativeBackoff();
    long maxMs = FlumeConfiguration.get().getFailoverMaxSingleBackoff();
    BackoffPolicy backoff1 = new CumulativeCappedExponentialBackoff(initMs,
        maxMs, cumulativeMaxMs);
    BackoffPolicy backoff2 = new CumulativeCappedExponentialBackoff(initMs,
        maxMs, cumulativeMaxMs);

    // the collector snk has ack checking logic, retry and reopen logic, and
    // needs an extra mask before rolling, writing to disk and forwarding acks
    // (roll detect).

    // { ackChecksumChecker => insistentAppend => stubbornAppend =>
    // insistentOpen => mask("rolltag") => roll(xx) { rollDetect =>
    // escapedCusomtDfs } }
    EventSink tmp = new MaskDecorator<EventSink>(roller, "rolltag");
    tmp = new InsistentOpenDecorator<EventSink>(tmp, backoff1);
    tmp = new StubbornAppendSink<EventSink>(tmp);
    tmp = new InsistentAppendDecorator<EventSink>(tmp, backoff2);
    snk = new AckChecksumChecker<EventSink>(tmp, accum);
  }

  String curRollTag;

  /**
   * This is a helper class that wraps the body of the collector sink, so that
   * and gives notifications when a roll hash happened. Because only close has
   * sane flushing semantics in hdfs <= v0.20.x we need to collect acks, data is
   * safe only after a close on the hdfs file happens.
   */
  class RollDetectDeco extends EventSinkDecorator<EventSink> {
    String tag;

    public RollDetectDeco(EventSink s, String tag) {
      super(s);
      this.tag = tag;
    }

    public void open() throws IOException, InterruptedException {
      // set the collector's current tag to curRollTAg.
      LOG.debug("opening roll detect deco {}", tag);
      curRollTag = tag;
      super.open();
      LOG.debug("opened  roll detect deco {}", tag);
    }

    @Override
    public void close() throws IOException, InterruptedException {
      LOG.debug("closing roll detect deco {}", tag);
      super.close();
      flushRollAcks();
      LOG.debug("closed  roll detect deco {}", tag);
    }

    void flushRollAcks() throws IOException {
      AckListener master = ackDest;
      Collection<String> acktags;
      synchronized (rollAckSet) {
        acktags = new ArrayList<String>(rollAckSet);
        rollAckSet.clear();
        LOG.debug("Roll closed, pushing acks for " + tag + " :: " + acktags);
      }

      for (String at : acktags) {
        master.end(at);
      }
    }

  };

  /**
   * This accumulates ack tags in rollAckMap so that they can be pushed to the
   * master when the the hdfs file associated with the rolltag is closed.
   */
  class AckAccumulator implements AckListener {

    @Override
    public void end(String group) throws IOException {
      synchronized (rollAckSet) {
        LOG.debug("Adding to acktag {} to rolltag {}", group, curRollTag);
        rollAckSet.add(group);
        LOG.debug("Current rolltag acktag mapping: {}", rollAckSet);
      }
    }

    @Override
    public void err(String group) throws IOException {
    }

    @Override
    public void expired(String key) throws IOException {
    }

    @Override
    public void start(String group) throws IOException {
    }

  };

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    snk.append(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    snk.close();
  }

  @Override
  public void open() throws IOException, InterruptedException {
    snk.open();
  }

  @Override
  public String getName() {
    return "Collector";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    map.put(snk.getName(), snk);
    return map;
  }

  @Deprecated
  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    snk.getReports(namePrefix + getName() + ".", reports);
  }

  public EventSink getSink() {
    return snk;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length <= 3 && argv.length >= 2,
            "usage: collectorSink[(dfsdir,path[,rollmillis])]");
        String logdir = FlumeConfiguration.get().getCollectorDfsDir(); // default
        long millis = FlumeConfiguration.get().getCollectorRollMillis();
        String prefix = "";
        if (argv.length >= 1) {
          logdir = argv[0]; // override
        }
        if (argv.length >= 2) {
          prefix = argv[1];
        }
        if (argv.length >= 3) {
          millis = Long.parseLong(argv[2]);
        }
        try {
          EventSink snk = new CollectorSink(context, logdir, prefix, millis,
              FlumeNode.getInstance().getCollectorAckListener());
          return snk;
        } catch (FlumeSpecException e) {
          LOG.error("CollectorSink spec error " + e, e);
          throw new IllegalArgumentException(
              "usage: collectorSink[(dfsdir,path[,rollmillis])]" + e);
        }
      }
    };
  }
}
