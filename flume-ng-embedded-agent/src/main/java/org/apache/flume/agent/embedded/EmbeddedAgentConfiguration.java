/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.agent.embedded;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.channel.ChannelType;
import org.apache.flume.conf.sink.SinkProcessorType;
import org.apache.flume.conf.sink.SinkType;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Stores publicly accessible configuration constants and private
 * configuration constants and methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EmbeddedAgentConfiguration {
  public static final String SEPERATOR = ".";
  private static final Joiner JOINER = Joiner.on(SEPERATOR);
  private static final String TYPE = "type";

  /**
   * Prefix for source properties
   */
  public static final String SOURCE = "source";
  /**
   * Prefix for channel properties
   */
  public static final String CHANNEL = "channel";
  /**
   * Prefix for sink processor properties
   */
  public static final String SINK_PROCESSOR = "processor";
  /**
   * Space delimited list of sink names: e.g. sink1 sink2 sink3
   */
  public static final String SINKS = "sinks";

  public static final String SINKS_PREFIX = join(SINKS, "");
  /**
   * Source type, choices are `embedded'
   */
  public static final String SOURCE_TYPE = join(SOURCE, TYPE);
  /**
   * Prefix for passing configuration parameters to the source
   */
  public static final String SOURCE_PREFIX = join(SOURCE, "");
  /**
   * Channel type, choices are `memory' or `file'
   */
  public static final String CHANNEL_TYPE = join(CHANNEL, TYPE);
  /**
   * Prefix for passing configuration parameters to the channel
   */
  public static final String CHANNEL_PREFIX = join(CHANNEL, "");
  /**
   * Sink processor type, choices are `default', `failover' or `load_balance'
   */
  public static final String SINK_PROCESSOR_TYPE = join(SINK_PROCESSOR, TYPE);
  /**
   * Prefix for passing configuration parameters to the sink processor
   */
  public static final String SINK_PROCESSOR_PREFIX = join(SINK_PROCESSOR, "");
  /**
   * Embedded source which provides simple in-memory transfer to channel.
   * Use this source via the put,putAll methods on the EmbeddedAgent. This
   * is the only supported source to use for Embedded Agents.
   */
  public static final String SOURCE_TYPE_EMBEDDED = EmbeddedSource.class.getName();
  private static final String SOURCE_TYPE_EMBEDDED_ALIAS = "EMBEDDED";
  /**
   * Memory channel which stores events in heap. See Flume User Guide for
   * configuration information. This is the recommended channel to use for
   * Embedded Agents.
   */
  public static final String CHANNEL_TYPE_MEMORY = ChannelType.MEMORY.name();

  /**
  * Spillable Memory channel which stores events in heap. See Flume User Guide for
  * configuration information. This is the recommended channel to use for
  * Embedded Agents.
   */
  public static final String CHANNEL_TYPE_SPILLABLEMEMORY = ChannelType.SPILLABLEMEMORY.name();

  /**
   * File based channel which stores events in on local disk. See Flume User
   * Guide for configuration information.
   */
  public static final String CHANNEL_TYPE_FILE = ChannelType.FILE.name();

  /**
   * Avro sink which can send events to a downstream avro source. This is the
   * only supported sink for Embedded Agents.
   */
  public static final String SINK_TYPE_AVRO = SinkType.AVRO.name();

  /**
   * Default sink processors which may be used when there is only a single sink.
   */
  public static final String SINK_PROCESSOR_TYPE_DEFAULT = SinkProcessorType.DEFAULT.name();
  /**
   * Failover sink processor. See Flume User Guide for configuration
   * information.
   */
  public static final String SINK_PROCESSOR_TYPE_FAILOVER = SinkProcessorType.FAILOVER.name();
  /**
   * Load balancing sink processor. See Flume User Guide for configuration
   * information.
   */
  public static final String SINK_PROCESSOR_TYPE_LOAD_BALANCE = SinkProcessorType.LOAD_BALANCE.name();


  private static final String[] ALLOWED_SOURCES = {
    SOURCE_TYPE_EMBEDDED_ALIAS,
    SOURCE_TYPE_EMBEDDED,
  };

  private static final String[] ALLOWED_CHANNELS = {
    CHANNEL_TYPE_MEMORY,
    CHANNEL_TYPE_FILE
  };

  private static final String[] ALLOWED_SINKS = {
    SINK_TYPE_AVRO
  };

  private static final String[] ALLOWED_SINK_PROCESSORS = {
    SINK_PROCESSOR_TYPE_DEFAULT,
    SINK_PROCESSOR_TYPE_FAILOVER,
    SINK_PROCESSOR_TYPE_LOAD_BALANCE
  };

  private static final ImmutableList<String> DISALLOWED_SINK_NAMES =
      ImmutableList.of("source", "channel", "processor");

  private static void validate(String name,
      Map<String, String> properties) throws FlumeException {

    if(properties.containsKey(SOURCE_TYPE)) {
      checkAllowed(ALLOWED_SOURCES, properties.get(SOURCE_TYPE));
    }
    checkRequired(properties, CHANNEL_TYPE);
    checkAllowed(ALLOWED_CHANNELS, properties.get(CHANNEL_TYPE));
    checkRequired(properties, SINKS);
    String sinkNames = properties.get(SINKS);
    for(String sink : sinkNames.split("\\s+")) {
      if(DISALLOWED_SINK_NAMES.contains(sink.toLowerCase())) {
        throw new FlumeException("Sink name " + sink + " is one of the" +
            " disallowed sink names: " + DISALLOWED_SINK_NAMES);
      }
      String key = join(sink, TYPE);
      checkRequired(properties, key);
      checkAllowed(ALLOWED_SINKS, properties.get(key));

    }
    checkRequired(properties, SINK_PROCESSOR_TYPE);
    checkAllowed(ALLOWED_SINK_PROCESSORS, properties.get(SINK_PROCESSOR_TYPE));
  }
  /**
   * Folds embedded configuration structure into an agent configuration.
   * Should only be called after validate returns without error.
   *
   * @param name - agent name
   * @param properties - embedded agent configuration
   * @return configuration applicable to a flume agent
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Map<String, String> configure(String name,
      Map<String, String> properties) throws FlumeException {
    validate(name, properties);
    // we are going to modify the properties as we parse the config
    properties = new HashMap<String, String>(properties);

    if(!properties.containsKey(SOURCE_TYPE) || SOURCE_TYPE_EMBEDDED_ALIAS.
        equalsIgnoreCase(properties.get(SOURCE_TYPE))) {
      properties.put(SOURCE_TYPE, SOURCE_TYPE_EMBEDDED);
    }
    String sinkNames = properties.remove(SINKS);

    String sourceName = "source-" + name;
    String channelName = "channel-" + name;
    String sinkGroupName = "sink-group-" + name;

    /*
     * Now we are going to process the user supplied configuration
     * and generate an agent configuration. This is only to supply
     * a simpler client api than passing in an entire agent configuration.
     */
    // user supplied config -> agent configuration
    Map<String, String> result = Maps.newHashMap();

    // properties will be modified during iteration so we need a
    // copy of the keys
    Set<String> userProvidedKeys;
    /*
     * First we are going to setup all the root level pointers. I.E
     * point the agent at the components, sink group at sinks, and
     * source at the channel.
     */
    // point agent at source
    result.put(join(name, BasicConfigurationConstants.CONFIG_SOURCES),
        sourceName);
    // point agent at channel
    result.put(join(name, BasicConfigurationConstants.CONFIG_CHANNELS),
        channelName);
    // point agent at sinks
    result.put(join(name, BasicConfigurationConstants.CONFIG_SINKS),
        sinkNames);
    // points the agent at the sinkgroup
    result.put(join(name, BasicConfigurationConstants.CONFIG_SINKGROUPS),
        sinkGroupName);
    // points the sinkgroup at the sinks
    result.put(join(name, BasicConfigurationConstants.CONFIG_SINKGROUPS,
            sinkGroupName, SINKS), sinkNames);
    // points the source at the channel
    result.put(join(name,
        BasicConfigurationConstants.CONFIG_SOURCES, sourceName,
        BasicConfigurationConstants.CONFIG_CHANNELS), channelName);
    /*
     * Second process the sink configuration and point the sinks
     * at the channel.
     */
    userProvidedKeys = new HashSet<String>(properties.keySet());
    for(String sink :  sinkNames.split("\\s+")) {
      for(String key : userProvidedKeys) {
        String value = properties.get(key);
        if(key.startsWith(sink + SEPERATOR)) {
          properties.remove(key);
          result.put(join(name,
              BasicConfigurationConstants.CONFIG_SINKS, key), value);
        }
      }
      // point the sink at the channel
      result.put(join(name,
          BasicConfigurationConstants.CONFIG_SINKS, sink,
          BasicConfigurationConstants.CONFIG_CHANNEL), channelName);
    }
    /*
     * Third, process all remaining configuration items, prefixing them
     * correctly and then passing them on to the agent.
     */
    userProvidedKeys = new HashSet<String>(properties.keySet());
    for(String key : userProvidedKeys) {
      String value = properties.get(key);
      if(key.startsWith(SOURCE_PREFIX)) {
        // users use `source' but agent needs the actual source name
        key = key.replaceFirst(SOURCE, sourceName);
        result.put(join(name,
            BasicConfigurationConstants.CONFIG_SOURCES, key), value);
      } else if(key.startsWith(CHANNEL_PREFIX)) {
        // users use `channel' but agent needs the actual channel name
        key = key.replaceFirst(CHANNEL, channelName);
        result.put(join(name,
            BasicConfigurationConstants.CONFIG_CHANNELS, key), value);
      } else if(key.startsWith(SINK_PROCESSOR_PREFIX)) {
        // agent.sinkgroups.sinkgroup.processor.*
        result.put(join(name, BasicConfigurationConstants.CONFIG_SINKGROUPS,
                sinkGroupName, key), value);
      } else {
        // XXX should we simply ignore this?
        throw new FlumeException("Unknown configuration " + key);
      }
    }
    return result;
  }
  private static void checkAllowed(String[] allowedTypes, String type) {
    boolean isAllowed = false;
    type = type.trim();
    for(String allowedType : allowedTypes) {
      if(allowedType.equalsIgnoreCase(type)) {
        isAllowed = true;
        break;
      }
    }
    if(!isAllowed) {
      throw new FlumeException("Component type of " + type + " is not in " +
          "allowed types of " + Arrays.toString(allowedTypes));
    }
  }
  private static void checkRequired(Map<String, String> properties,
      String name) {
    if(!properties.containsKey(name)) {
      throw new FlumeException("Required parameter not found " + name);
    }
  }

  private static String join(String... parts) {
    return JOINER.join(parts);
  }

  private EmbeddedAgentConfiguration() {

  }
}