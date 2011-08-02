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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.AgentFailChainSink;
import com.cloudera.flume.agent.AgentSink;
import com.cloudera.flume.agent.diskfailover.DiskFailoverDeco;
import com.cloudera.flume.agent.durability.NaiveFileWALDeco;
import com.cloudera.flume.collector.CollectorSink;
import com.cloudera.flume.core.DigestDecorator;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.FormatterDecorator;
import com.cloudera.flume.core.MaskDecorator;
import com.cloudera.flume.core.SelectDecorator;
import com.cloudera.flume.core.extractors.RegexExtractor;
import com.cloudera.flume.core.extractors.SplitExtractor;
import com.cloudera.flume.handlers.avro.AvroEventSink;
import com.cloudera.flume.handlers.batch.BatchingDecorator;
import com.cloudera.flume.handlers.batch.GunzipDecorator;
import com.cloudera.flume.handlers.batch.GzipDecorator;
import com.cloudera.flume.handlers.batch.UnbatchingDecorator;
import com.cloudera.flume.handlers.debug.BenchmarkInjectDecorator;
import com.cloudera.flume.handlers.debug.BenchmarkReportDecorator;
import com.cloudera.flume.handlers.debug.BloomCheckDecorator;
import com.cloudera.flume.handlers.debug.BloomGeneratorDeco;
import com.cloudera.flume.handlers.debug.ChokeDecorator;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.debug.DelayDecorator;
import com.cloudera.flume.handlers.debug.FlakeyEventSink;
import com.cloudera.flume.handlers.debug.InMemoryDecorator;
import com.cloudera.flume.handlers.debug.InsistentAppendDecorator;
import com.cloudera.flume.handlers.debug.InsistentOpenDecorator;
import com.cloudera.flume.handlers.debug.IntervalDroppyEventSink;
import com.cloudera.flume.handlers.debug.IntervalFlakeyEventSink;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.handlers.debug.MultiplierDecorator;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.debug.StubbornAppendSink;
import com.cloudera.flume.handlers.debug.TextFileSink;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.ValueDecorator;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.hdfs.DFSEventSink;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.irc.IrcSink;
import com.cloudera.flume.handlers.rpc.RpcSink;
import com.cloudera.flume.handlers.syslog.SyslogTcpSink;
import com.cloudera.flume.handlers.thrift.ThriftAckedEventSink;
import com.cloudera.flume.handlers.thrift.ThriftEventSink;
import com.cloudera.flume.handlers.thrift.ThriftRawEventSink;
import com.cloudera.flume.master.availability.FailoverChainSink;
import com.cloudera.flume.reporter.aggregator.AccumulatorSink;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.ganglia.GangliaSink;
import com.cloudera.flume.reporter.histogram.MultiGrepReporterSink;
import com.cloudera.flume.reporter.histogram.RegexGroupHistogramSink;
import com.cloudera.flume.reporter.history.CountHistoryReporter;
import com.cloudera.flume.reporter.sampler.IntervalSampler;
import com.cloudera.flume.reporter.sampler.ProbabilitySampler;
import com.cloudera.flume.reporter.sampler.ReservoirSamplerDeco;
import com.cloudera.util.Pair;

/**
 * This factory builds sink and sink decorators. This implementation requires a
 * recompile to add new types.
 */
public class SinkFactoryImpl extends SinkFactory {
  static final Logger LOG = LoggerFactory.getLogger(SinkFactoryImpl.class);

  // The actual types are <String, SinkBuilder>
  static Object[][] sinkList = {
      // high level sinks.
      { "collectorSink", CollectorSink.hdfsBuilder() },

      { "agentSink", AgentSink.e2eBuilder() },
      { "agentE2ESink", AgentSink.e2eBuilder() }, // now with acks
      { "agentDFOSink", AgentSink.dfoBuilder() },
      { "agentBESink", AgentSink.beBuilder() },

      { "agentFailoverSink", AgentSink.dfoBuilder() },
      { "agentBestEffortSink", AgentSink.beBuilder() },

      { "agentE2EChain", AgentFailChainSink.e2eBuilder() },
      { "agentDFOChain", AgentFailChainSink.dfoBuilder() },
      { "agentBEChain", AgentFailChainSink.beBuilder() },

      // autoE2EChain, autoDFOChain and autoBEChains are essentially node
      // specific "macros", and use let expression shadowing
      { "autoBEChain", EventSink.StubSink.builder("autoBEChain") },
      { "autoDFOChain", EventSink.StubSink.builder("autoDFOChain") },
      { "autoE2EChain", EventSink.StubSink.builder("autoE2EChain") },
      { "logicalSink", EventSink.StubSink.builder("logicalSink") },

      // low level sinks
      { "null", NullSink.builder() },
      // all calls throw exception
      { "fail", EventSink.StubSink.builder("fail") },

      { "console", ConsoleEventSink.builder() },
      { "text", TextFileSink.builder() },
      { "seqfile", SeqfileEventSink.builder() },
      { "dfs", DFSEventSink.builder() }, // escapes
      { "customdfs", CustomDfsSink.builder() }, // does not escape
      { "escapedCustomDfs", EscapedCustomDfsSink.builder() }, // escapes
      { "rpcSink", RpcSink.builder() }, // creates AvroEventSink or
      // ThriftEventSink
      { "syslogTcp", SyslogTcpSink.builder() },
      { "irc", IrcSink.builder() },
      { "thriftSink", ThriftEventSink.builder() },
      { "avroSink", AvroEventSink.builder() },
      // advanced
      { "failChain", FailoverChainSink.builder() }, // @deprecated

      // reports
      { "ganglia", GangliaSink.builder() },
      { "counter", CounterSink.builder() },
      { "accumulator", AccumulatorSink.builder() },
      { "counterHistory", CountHistoryReporter.builder() },
      { "multigrep", MultiGrepReporterSink.builderSimple() },
      { "multigrepspec", MultiGrepReporterSink.builder() },
      { "regexhisto", RegexGroupHistogramSink.builderSimple() },
      { "regexhistospec", RegexGroupHistogramSink.builder() },

      // deprecated
      { "tsink", ThriftEventSink.builder() },
      { "tacksink", ThriftAckedEventSink.builder() },
      { "trawsink", ThriftRawEventSink.builder() }, };

  // The actual types are <String, SinkDecoBuilder>
  static Object[][] decoList = {
      { "nullDeco", EventSinkDecorator.nullBuilder() },
      { "diskFailover", DiskFailoverDeco.builder() },
      { "ackedWriteAhead", NaiveFileWALDeco.builderEndToEndDir() },

      { "ackChecker", AckChecksumChecker.builder() },
      { "ackInjector", AckChecksumInjector.builder() },

      { "lazyOpen", LazyOpenDecorator.builder() },
      { "insistentOpen", InsistentOpenDecorator.builder() },
      { "insistentAppend", InsistentAppendDecorator.builder() },
      { "stubbornAppend", StubbornAppendSink.builder() },

      // relational algebra projection
      { "value", ValueDecorator.builder() },
      { "mask", MaskDecorator.builder() },
      { "select", SelectDecorator.builder() },

      // TODO (jon) relational algebra selection (filtering)

      // format the output
      { "format", FormatterDecorator.builder() },

      // message digest of body
      { "digest", DigestDecorator.builder() },

      // extractors
      { "regex", RegexExtractor.builder() },
      { "split", SplitExtractor.builder() },

      // cpu / network tradeoffs
      { "batch", BatchingDecorator.builder() },
      { "unbatch", UnbatchingDecorator.builder() },
      { "gzip", GzipDecorator.builder() },
      { "gunzip", GunzipDecorator.builder() },

      // sampling
      { "intervalSampler", IntervalSampler.builder() },
      { "probSampler", ProbabilitySampler.builder() },
      { "reservoirSampler", ReservoirSamplerDeco.builder() },

      // debugging related
      { "flakeyAppend", FlakeyEventSink.builder() },
      { "intervalFlakeyAppend", IntervalFlakeyEventSink.builder() },
      { "intervalDroppyAppend", IntervalDroppyEventSink.builder() },

      { "inmem", InMemoryDecorator.builder() },
      { "benchinject", BenchmarkInjectDecorator.builder() },
      { "benchreport", BenchmarkReportDecorator.builder() },
      { "bloomGen", BloomGeneratorDeco.builder() },
      { "bloomCheck", BloomCheckDecorator.builder() },
      { "mult", MultiplierDecorator.builder() },
      { "delay", DelayDecorator.builder() },
      { "choke", ChokeDecorator.builder() },

  };

  Map<String, SinkBuilder> sinks = new HashMap<String, SinkBuilder>();
  Map<String, SinkDecoBuilder> decos = new HashMap<String, SinkDecoBuilder>();

  public SinkFactoryImpl() {
    for (Object[] entry : sinkList) {
      String key = (String) entry[0];
      SinkBuilder value = (SinkBuilder) entry[1];
      sinks.put(key, value);
    }

    for (Object[] entry : decoList) {
      String key = (String) entry[0];
      SinkDecoBuilder value = (SinkDecoBuilder) entry[1];
      decos.put(key, value);
    }

    String classes = FlumeConfiguration.get().getPluginClasses();
    if (!classes.equals("")) {
      for (String s : classes.split(",")) {
        loadPluginBuilders(s);
      }
    }
  }

  @Override
  public Set<String> getSinkNames() {
    return Collections.unmodifiableSet(sinks.keySet());
  }

  @Override
  public Set<String> getDecoratorNames() {
    return Collections.unmodifiableSet(decos.keySet());
  }

  /**
   * Given a fully qualified classname, load the class it represents and try and
   * invoke two methods to get a list of sinks and builders.
   * 
   * This should only be called at startup to avoid dependency issues; there is
   * no lazy loading.
   * 
   * Any exceptions are simply logged and ignored. This is an undocumented
   * feature for now.
   */
  @SuppressWarnings("unchecked")
  protected void loadPluginBuilders(String clsName) {
    try {
      Class<Object> cls = (Class<Object>) Class.forName(clsName);
      try {
        Method mth = cls.getMethod("getSinkBuilders");
        List<Pair<String, SinkBuilder>> builders = (List<Pair<String, SinkBuilder>>) mth
            .invoke(mth);
        for (Pair<String, SinkBuilder> snk : builders) {
          LOG.info("Found sink builder " + snk.getLeft() + " in " + clsName);
          sinks.put(snk.getLeft(), snk.getRight());
        }
      } catch (NoSuchMethodException e) {
        LOG.warn("No sink builders found in " + clsName);
      } catch (Exception e) {
        LOG.error("Error invoking getSinkBuilders on class " + clsName, e);
      }
      try {
        Method mth = cls.getMethod("getDecoratorBuilders");
        List<Pair<String, SinkDecoBuilder>> builders = (List<Pair<String, SinkDecoBuilder>>) mth
            .invoke(mth);
        for (Pair<String, SinkDecoBuilder> snk : builders) {
          LOG.info("Found sink decorator " + snk.getLeft() + " in " + clsName);
          decos.put(snk.getLeft(), snk.getRight());
        }
      } catch (NoSuchMethodException e) {
        LOG.warn("No sink decorators found in " + clsName);
      } catch (Exception e) {
        LOG.error("Error invoking getDecoratorBuilders on class " + clsName, e);
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Could not find class " + clsName + " for plugin loading", e);
    }
  }

  @Override
  public EventSinkDecorator<EventSink> getDecorator(Context context,
      String name, String... args) throws FlumeSpecException {
    try {
      SinkDecoBuilder builder = decos.get(name);
      if (builder == null)
        return null;
      return builder.build(context, args);
    } catch (NumberFormatException nfe) {
      throw new FlumeArgException("Illegal number format: " + nfe.getMessage());
    } catch (IllegalArgumentException iae) {
      throw new FlumeArgException(iae.getMessage());
    }
  }

  @Override
  public EventSink getSink(Context context, String name, String... args)
      throws FlumeSpecException {
    try {
      SinkBuilder builder = sinks.get(name);
      if (builder == null)
        return null;
      return builder.build(context, args);
    } catch (NumberFormatException nfe) {
      throw new FlumeArgException("Illegal number format: " + nfe.getMessage());
    } catch (IllegalArgumentException iae) {
      throw new FlumeArgException(iae.getMessage());
    }
  }

  /**
   * This is only for testing
   */
  public void setSink(String name, SinkBuilder builder) {
    sinks.put(name, builder);
  }

  /**
   * This is only for testing
   */
  public void setDeco(String name, SinkDecoBuilder builder) {
    decos.put(name, builder);
  }
}
