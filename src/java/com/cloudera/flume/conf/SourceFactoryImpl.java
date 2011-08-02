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

import com.cloudera.flume.collector.CollectorSource;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.PollingSource;
import com.cloudera.flume.core.EventSource.StubSource;
import com.cloudera.flume.handlers.avro.AvroEventSource;
import com.cloudera.flume.handlers.console.JLineStdinSource;
import com.cloudera.flume.handlers.debug.Log4jTextFileSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.debug.NoNlSynthSource;
import com.cloudera.flume.handlers.debug.NullSource;
import com.cloudera.flume.handlers.debug.StdinSource;
import com.cloudera.flume.handlers.debug.SynthSource;
import com.cloudera.flume.handlers.debug.SynthSourceRndSize;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.handlers.exec.ExecEventSource;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSource;
import com.cloudera.flume.handlers.irc.IrcSource;
import com.cloudera.flume.handlers.rpc.RpcSource;
import com.cloudera.flume.handlers.scribe.ScribeEventSource;
import com.cloudera.flume.handlers.syslog.SyslogTcpSource;
import com.cloudera.flume.handlers.syslog.SyslogTcpSourceThreads;
import com.cloudera.flume.handlers.syslog.SyslogUdpSource;
import com.cloudera.flume.handlers.text.TailDirSource;
import com.cloudera.flume.handlers.text.TailSource;
import com.cloudera.flume.handlers.thrift.PrioritizedThriftEventSource;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.cloudera.flume.handlers.twitter.TwitterStreamSource;
import com.cloudera.util.Pair;

/**
 * This factory creates event sources. It currently requires a recompile when
 * new sources are added.
 */
public class SourceFactoryImpl extends SourceFactory {
  static final Logger LOG = LoggerFactory.getLogger(SourceFactoryImpl.class);

  static Object[][] sourceList = {
      // high level sources
      { "logicalSource", StubSource.builder() },
      { "autoCollectorSource", StubSource.builder(0, 0) },// no args allowed
      { "collectorSource", CollectorSource.builder() },
      { "fail", StubSource.builder() },

      // low level Sources
      { "null", NullSource.builder() },
      { "stdin", StdinSource.builder() },
      { "console", JLineStdinSource.builder() },

      // creates AvroEventSource or ThriftEventSource
      { "rpcSource", RpcSource.builder() },
      { "thriftSource", ThriftEventSource.builder() },
      { "avroSource", AvroEventSource.builder() },
      { "tSource", ThriftEventSource.builder() },
      { "text", TextFileSource.builder() }, { "tail", TailSource.builder() },
      { "multitail", TailSource.multiTailBuilder() },
      { "tailDir", TailDirSource.builder() },
      { "seqfile", SeqfileEventSource.builder() },
      { "syslogUdp", SyslogUdpSource.builder() },
      { "syslogTcp", SyslogTcpSourceThreads.builder() },
      { "syslogTcp1", SyslogTcpSource.builder() },
      { "execPeriodic", ExecEventSource.buildPeriodic() },
      { "execStream", ExecEventSource.buildStream() },
      { "exec", ExecEventSource.builder() },
      { "synth", SynthSource.builder() },
      { "nonlsynth", NoNlSynthSource.builder() },
      { "asciisynth", NoNlASCIISynthSource.builder() },
      { "synthrndsize", SynthSourceRndSize.builder() },
      { "scribe", ScribeEventSource.builder() },
      { "report", PollingSource.reporterPollBuilder() },

      // fun but unsupported officially.
      { "twitter", TwitterStreamSource.builder() },
      { "irc", IrcSource.builder() },

      // experimental / Cloudera SA only.
      { "tpriosource", PrioritizedThriftEventSource.builder() },

      // TODO (jon) deprecate these, use format, make arg to
      // text/tail/console

      { "log4jfile", Log4jTextFileSource.builder() }, };

  Map<String, SourceBuilder> sources = new HashMap<String, SourceBuilder>();

  public SourceFactoryImpl() {
    for (Object[] entry : sourceList) {
      String key = (String) entry[0];
      SourceBuilder value = (SourceBuilder) entry[1];
      sources.put(key, value);
    }

    String classes = FlumeConfiguration.get().getPluginClasses();
    if (!classes.equals("")) {
      for (String s : classes.split(",")) {
        loadPluginBuilders(s);
      }
    }
  }

  @Override
  public EventSource getSource(String name, String... args)
      throws FlumeSpecException {
    try {
      SourceBuilder builder = sources.get(name);
      if (builder == null) {
        return null;
      }
      return builder.build(args);
    } catch (NumberFormatException nfe) {
      throw new FlumeArgException("Illegal number format: " + nfe.getMessage());
    } catch (IllegalArgumentException iae) {
      throw new FlumeSpecException(iae.getMessage());
    }
  }

  /**
   * Given a fully qualified classname, load the class it represents and try and
   * invoke two methods to get a list of sources.
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
        Method mth = cls.getMethod("getSourceBuilders");
        List<Pair<String, SourceBuilder>> builders = (List<Pair<String, SourceBuilder>>) mth
            .invoke(mth);
        for (Pair<String, SourceBuilder> snk : builders) {
          LOG.info("Found source builder " + snk.getLeft() + " in " + clsName);
          sources.put(snk.getLeft(), snk.getRight());
        }
      } catch (NoSuchMethodException e) {
        LOG.warn("No source builders found in " + clsName);
      } catch (Exception e) {
        LOG.error("Error invoking getSourceBuilders on class " + clsName, e);
      }
    } catch (ClassNotFoundException e) {
      LOG.error("Could not find class " + clsName + " for plugin loading", e);
    }
  }

  /**
   * This is for testing only. It allows us to add arbitrary sources to the
   * builder.
   */
  public void setSource(String name, SourceBuilder builder) {
    sources.put(name, builder);
  }

  @Override
  public Set<String> getSourceNames() {
    return Collections.unmodifiableSet(sources.keySet());
  }
}
