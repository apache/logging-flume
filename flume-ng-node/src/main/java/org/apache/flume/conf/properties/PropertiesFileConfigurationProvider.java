/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.conf.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ChannelSelectorFactory;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.file.AbstractFileConfigurationProvider;
import org.apache.flume.conf.file.SimpleNodeConfiguration;
import org.apache.flume.conf.properties.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.conf.properties.FlumeConfiguration.ComponentConfiguration;
import org.apache.flume.node.NodeConfiguration;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.SinkGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A configuration provider that uses properties file for specifying
 * configuration. The configuration files follow the Java properties file syntax
 * rules specified at {@link java.util.Properties#load(java.io.Reader)}. Every
 * configuration value specified in the properties file is prefixed by an
 * <em>Agent Name</em> which helps isolate an individual agent&apos;s namespace.
 * </p>
 * <p>
 * Valid configuration files must observe the following rules for every agent
 * namespace.
 * <ul>
 * <li>For every &lt;agent name&gt; there must be three lists specified that
 * include <tt>&lt;agent name&gt;.sources</tt>,
 * <tt>&lt;agent name&gt;.sinks</tt>, and <tt>&lt;agent name&gt;.channels</tt>.
 * Each of these lists must contain a space separated list of names
 * corresponding to that particular entity.</li>
 * <li>For each source named in <tt>&lt;agent name&gt;.sources</tt>, there must
 * be a non-empty <tt>type</tt> attribute specified from the valid set of source
 * types. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.type = event</tt></li>
 * <li>For each source named in <tt>&lt;agent name&gt;.sources</tt>, there must
 * be a space-separated list of channel names that the source will associate
 * with during runtime. Each of these names must be contained in the channels
 * list specified by <tt>&lt;agent name&gt;.channels</tt>. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.channels =
 * &lt;channel-1 name&gt; &lt;channel-2 name&gt;</tt></li>
 * <li>For each source named in the <tt>&lt;agent name&gt;.sources</tt>, there
 * must be a <tt>runner</tt> namespace of configuration that configures the
 * associated source runner. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.runner.type = avro</tt>.
 * This namespace can also be used to configure other configuration of the
 * source runner as needed. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.runner.port = 10101</tt>
 * </li>
 * <li>For each source named in <tt>&lt;sources&gt;.sources</tt> there can
 * be an optional <tt>selector.type</tt> specified that identifies the type
 * of channel selector associated with the source. If not specified, the
 * default replicating channel selector is used.
 * </li><li>For each channel named in the <tt>&lt;agent name&gt;.channels</tt>,
 * there must be a non-empty <tt>type</tt> attribute specified from the valid
 * set of channel types. For example:
 * <tt>&lt;agent name&gt;.channels.&lt;channel name&gt;.type = mem</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a non-empty <tt>type</tt> attribute specified from the valid set of sink
 * types. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.type = hdfs</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a non-empty single-valued channel name specified as the value of the
 * <tt>channel</tt> attribute. This value must be contained in the channels list
 * specified by <tt>&lt;agent name&gt;.channels</tt>. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.channel =
 * &lt;channel name&gt;</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a <tt>runner</tt> namespace of configuration that configures the
 * associated sink runner. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.runner.type = polling</tt>.
 * This namespace can also be used to configure other configuration of the sink
 * runner as needed. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.runner.polling.interval =
 * 60</tt></li>
 * <li>A fourth optional list <tt>&lt;agent name&gt;.sinkgroups</tt>
 * may be added to each agent, consisting of unique space separated names 
 * for groups</li>
 * <li>Each sinkgroup must specify sinks, containing a list of all sinks 
 * belonging to it. These cannot be shared by multiple groups.
 * Further, one can set a processor and behavioral parameters to determine
 * how sink selection is made via <tt>&lt;agent name&gt;.sinkgroups.&lt;
 * group name&lt.processor</tt>. For further detail refer to inividual processor
 * documentation</li>
 * <li>Sinks not assigned to a group will be assigned to default single sink
 * groups.</li>
 * </ul>
 *
 * Apart from the above required configuration values, each source, sink or
 * channel can have its own set of arbitrary configuration as required by the
 * implementation. Each of these configuration values are expressed by fully
 * namespace qualified configuration keys. For example, the configuration
 * property called <tt>capacity</tt> for a channel called <tt>ch1</tt> for the
 * agent named <tt>host1</tt> with value <tt>1000</tt> will be expressed as:
 * <tt>host1.channels.ch1.capacity = 1000</tt>.
 * </p>
 * <p>
 * Any information contained in the configuration file other than what pertains
 * to the configured agents, sources, sinks and channels via the explicitly
 * enumerated list of sources, sinks and channels per agent name are ignored by
 * this provider. Moreover, if any of the required configuration values are not
 * present in the configuration file for the configured entities, that entity
 * and anything that depends upon it is considered invalid and consequently not
 * configured. For example, if a channel is missing its <tt>type</tt> attribute,
 * it is considered misconfigured. Also, any sources or sinks that depend upon
 * this channel are also considered misconfigured and not initialized.
 * </p>
 * <p>
 * Example configuration file:
 *
 * <pre>
 * #
 * # Flume Configuration
 * # This file contains configuration for one Agent identified as host1.
 * #
 *
 * host1.sources = avroSource thriftSource
 * host1.channels = jdbcChannel
 * host1.sinks = hdfsSink
 *
 * # avroSource configuration
 * host1.sources.avroSource.type = org.apache.flume.source.AvroSource
 * host1.sources.avroSource.runner.type = avro
 * host1.sources.avroSource.runner.port = 11001
 * host1.sources.avroSource.channels = jdbcChannel
 * host1.sources.avroSource.selector.type = replicating
 *
 * # thriftSource configuration
 * host1.sources.thriftSource.type = org.apache.flume.source.ThriftSource
 * host1.sources.thriftSource.runner.type = thrift
 * host1.sources.thriftSource.runner.port = 12001
 * host1.sources.thriftSource.channels = jdbcChannel
 *
 * # jdbcChannel configuration
 * host1.channels.jdbcChannel.type = jdbc
 * host1.channels.jdbcChannel.jdbc.driver = com.mysql.jdbc.Driver
 * host1.channels.jdbcChannel.jdbc.connect.url = http://localhost/flumedb
 * host1.channels.jdbcChannel.jdbc.username = flume
 * host1.channels.jdbcChannel.jdbc.password = flume
 *
 * # hdfsSink configuration
 * host1.sinks.hdfsSink.type = hdfs
 * host1.sinks.hdfsSink.namenode = hdfs://localhost/
 * host1.sinks.hdfsSink.batchsize = 1000
 * host1.sinks.hdfsSink.runner.type = polling
 * host1.sinks.hdfsSink.runner.polling.interval = 60
 * </pre>
 *
 * </p>
 *
 * @see java.util.Properties#load(java.io.Reader)
 */
public class PropertiesFileConfigurationProvider extends
AbstractFileConfigurationProvider {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(PropertiesFileConfigurationProvider.class);

  @Override
  protected void load() {
    File propertiesFile = getFile();
    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new FileReader(propertiesFile));
      Properties properties = new Properties();
      properties.load(reader);

      NodeConfiguration conf = new SimpleNodeConfiguration();
      FlumeConfiguration fconfig = new FlumeConfiguration(properties);
      AgentConfiguration agentConf = fconfig.getConfigurationFor(getNodeName());

      if (agentConf != null) {
        loadChannels(agentConf, conf);
        loadSources(agentConf, conf);
        loadSinks(agentConf, conf);

        getConfigurationAware().onNodeConfigurationChanged(conf);
      } else {
        LOGGER.warn("No configuration found for this host:{}", getNodeName());
      }
    } catch (IOException ex) {
      LOGGER.error("Unable to load file:" + propertiesFile
          + " (I/O failure) - Exception follows.", ex);
    } catch (InstantiationException ex) {
      LOGGER.error("Unable to load file:" + propertiesFile
          + " (failed to instantiate component) - Exception follows.", ex);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
          LOGGER.warn(
              "Unable to close file reader for file: " + propertiesFile, ex);
        }
      }
    }
  }

  private void loadChannels(AgentConfiguration agentConf,
      NodeConfiguration conf) throws InstantiationException {

    for (ComponentConfiguration comp : agentConf.getChannels()) {
      Context context = new Context();

      Channel channel = getChannelFactory().create(comp.getComponentName(),
          comp.getConfiguration().get("type"));

      for (Entry<String, String> entry : comp.getConfiguration().entrySet()) {
        context.put(entry.getKey(), entry.getValue());
      }

      Configurables.configure(channel, context);

      conf.getChannels().put(comp.getComponentName(), channel);
    }
  }

  private void loadSources(AgentConfiguration agentConf, NodeConfiguration conf)
      throws InstantiationException {

    for (ComponentConfiguration comp : agentConf.getSources()) {
      Context context = new Context();

      Map<String, String> componentConfig = comp.getConfiguration();

      Source source = getSourceFactory().create(comp.getComponentName(),
          componentConfig.get("type"));

      for (Entry<String, String> entry : comp.getConfiguration().entrySet()) {
        context.put(entry.getKey(), entry.getValue());
      }

      Configurables.configure(source, context);

      String channelNames = comp.getConfiguration().get("channels");
      List<Channel> channels = new ArrayList<Channel>();

      for (String chName : channelNames.split(" ")) {
        channels.add(conf.getChannels().get(chName));
      }

      Map<String,String> selectorConfig = comp.getSubconfiguration("selector");

      ChannelSelector selector = ChannelSelectorFactory.create(
          channels, selectorConfig);

      ChannelProcessor channelProcessor = new ChannelProcessor(selector);

      source.setChannelProcessor(channelProcessor);
      conf.getSourceRunners().put(comp.getComponentName(),
          SourceRunner.forSource(source));
    }
  }

  private void loadSinks(AgentConfiguration agentConf, NodeConfiguration conf)
      throws InstantiationException {

    Map<String, Sink> sinks = new HashMap<String, Sink>();
    for (ComponentConfiguration comp : agentConf.getSinks()) {
      Context context = new Context();
      Map<String, String> componentConfig = comp.getConfiguration();


      Sink sink = getSinkFactory().create(comp.getComponentName(),
          componentConfig.get("type"));

      for (Entry<String, String> entry : comp.getConfiguration().entrySet()) {
        context.put(entry.getKey(), entry.getValue());
      }

      Configurables.configure(sink, context);

      sink.setChannel(conf.getChannels().get(
          componentConfig.get("channel")));
      sinks.put(comp.getComponentName(), sink);
    }

    loadSinkGroups(agentConf, sinks, conf);
  }

  private void loadSinkGroups(AgentConfiguration agentConf,
      Map<String, Sink> sinks, NodeConfiguration conf)
          throws InstantiationException {
    Map<String, String> usedSinks = new HashMap<String, String>();
    for (ComponentConfiguration comp : agentConf.getSinkGroups()) {
      Context context = new Context();
      String groupName = comp.getComponentName();
      Map<String, String> groupConf = comp.getConfiguration();
      for (Entry<String, String> ent : groupConf.entrySet()) {
        context.put(ent.getKey(), ent.getValue());
      }
      String groupSinkList = groupConf.get("sinks");
      StringTokenizer sinkTokenizer = new StringTokenizer(groupSinkList, " \t");
      List<Sink> groupSinks = new ArrayList<Sink>();
      while(sinkTokenizer.hasMoreTokens()) {
        String sinkName = sinkTokenizer.nextToken();
        Sink s = sinks.remove(sinkName);
        if(s == null) {
          String sinkUser = usedSinks.get(sinkName);
          if(sinkUser != null) {
            throw new InstantiationException(String.format(
                "Sink %s of group %s already " +
                "in use by group %s", sinkName, groupName, sinkUser));
          } else {
            throw new InstantiationException(String.format(
                "Sink %s of group %s does "
                    + "not exist or is not properly configured", sinkName,
                groupName));
          }
        }
        groupSinks.add(s);
        usedSinks.put(sinkName, groupName);
      }
      SinkGroup group = new SinkGroup(groupSinks);
      Configurables.configure(group, context);
      conf.getSinkRunners().put(comp.getComponentName(),
          new SinkRunner(group.getProcessor()));
    }
    // add any unasigned sinks to solo collectors
    for(Entry<String, Sink> entry : sinks.entrySet()) {
      if (!usedSinks.containsValue(entry.getKey())) {
        SinkProcessor pr = new DefaultSinkProcessor();
        List<Sink> sinkMap = new ArrayList<Sink>();
        sinkMap.add(entry.getValue());
        pr.setSinks(sinkMap);
        Configurables.configure(pr, new Context());
        conf.getSinkRunners().put(entry.getKey(),
            new SinkRunner(pr));
      }
    }
  }

}
