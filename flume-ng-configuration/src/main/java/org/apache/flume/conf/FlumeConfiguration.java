/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.conf;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration.ComponentType;
import org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning;
import org.apache.flume.conf.channel.ChannelConfiguration;
import org.apache.flume.conf.channel.ChannelType;
import org.apache.flume.conf.sink.SinkConfiguration;
import org.apache.flume.conf.sink.SinkGroupConfiguration;
import org.apache.flume.conf.sink.SinkType;
import org.apache.flume.conf.source.SourceConfiguration;
import org.apache.flume.conf.source.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * <p>
 * FlumeConfiguration is an in memory representation of the hierarchical
 * configuration namespace required by the ConfigurationProvider.
 * This class is instantiated with a map or properties object which is parsed
 * to construct the hierarchy in memory. Once the entire set of properties have
 * been parsed and populated, a validation routine is run that identifies and
 * removes invalid components.
 * </p>
 *
 * @see org.apache.flume.node.ConfigurationProvider
 *
 */
public class FlumeConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(FlumeConfiguration.class);

  private final Map<String, AgentConfiguration> agentConfigMap;
  private final LinkedList<FlumeConfigurationError> errors;
  public static final String NEWLINE = System.getProperty("line.separator", "\n");
  public static final String INDENTSTEP = "  ";

  /**
   * Creates a populated Flume Configuration object.
   * @deprecated please use the other constructor
   */
  @Deprecated
  public FlumeConfiguration(Properties properties) {
    agentConfigMap = new HashMap<String, AgentConfiguration>();
    errors = new LinkedList<FlumeConfigurationError>();
    // Construct the in-memory component hierarchy
    for (Object name : properties.keySet()) {
      Object value = properties.get(name);
      if (!addRawProperty(name.toString(), value.toString())) {
        logger.warn("Configuration property ignored: " + name + " = " + value);
      }
    }
    // Now iterate thru the agentContext and create agent configs and add them
    // to agentConfigMap

    // validate and remove improperly configured components
    validateConfiguration();
  }

  /**
   * Creates a populated Flume Configuration object.
   */
  public FlumeConfiguration(Map<String, String> properties) {
    agentConfigMap = new HashMap<String, AgentConfiguration>();
    errors = new LinkedList<FlumeConfigurationError>();
    // Construct the in-memory component hierarchy
    for (String name : properties.keySet()) {
      String value = properties.get(name);

      if (!addRawProperty(name, value)) {
        logger.warn("Configuration property ignored: " + name + " = " + value);
      }
    }
    // Now iterate thru the agentContext and create agent configs and add them
    // to agentConfigMap

    // validate and remove improperly configured components
    validateConfiguration();
  }

  public List<FlumeConfigurationError> getConfigurationErrors() {
    return errors;
  }

  public AgentConfiguration getConfigurationFor(String hostname) {
    return agentConfigMap.get(hostname);
  }

  private void validateConfiguration() {
    Iterator<String> it = agentConfigMap.keySet().iterator();

    while (it.hasNext()) {
      String agentName = it.next();
      AgentConfiguration aconf = agentConfigMap.get(agentName);

      if (!aconf.isValid()) {
        logger.warn("Agent configuration invalid for agent '" + agentName
            + "'. It will be removed.");
        errors.add(new FlumeConfigurationError(agentName, "",
            FlumeConfigurationErrorType.AGENT_CONFIGURATION_INVALID,
            ErrorOrWarning.ERROR));

        it.remove();
      }
      logger.debug("Channels:" + aconf.channels + "\n");
      logger.debug("Sinks " + aconf.sinks + "\n");
      logger.debug("Sources " + aconf.sources + "\n");
    }

    logger.info("Post-validation flume configuration contains configuration"
        + " for agents: " + agentConfigMap.keySet());
  }

  private boolean addRawProperty(String name, String value) {
    // Null names and values not supported
    if (name == null || value == null) {
      errors
          .add(new FlumeConfigurationError("", "",
              FlumeConfigurationErrorType.AGENT_NAME_MISSING,
              ErrorOrWarning.ERROR));
      return false;
    }

    // Empty values are not supported
    if (value.trim().length() == 0) {
      errors
          .add(new FlumeConfigurationError(name, "",
              FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
              ErrorOrWarning.ERROR));
      return false;
    }

    // Remove leading and trailing spaces
    name = name.trim();
    value = value.trim();

    int index = name.indexOf('.');

    // All configuration keys must have a prefix defined as agent name
    if (index == -1) {
      errors
          .add(new FlumeConfigurationError(name, "",
              FlumeConfigurationErrorType.AGENT_NAME_MISSING,
              ErrorOrWarning.ERROR));
      return false;
    }

    String agentName = name.substring(0, index);

    // Agent name must be specified for all properties
    if (agentName.length() == 0) {
      errors
          .add(new FlumeConfigurationError(name, "",
              FlumeConfigurationErrorType.AGENT_NAME_MISSING,
              ErrorOrWarning.ERROR));
      return false;
    }

    String configKey = name.substring(index + 1);

    // Configuration key must be specified for every property
    if (configKey.length() == 0) {
      errors
          .add(new FlumeConfigurationError(name, "",
              FlumeConfigurationErrorType.PROPERTY_NAME_NULL,
              ErrorOrWarning.ERROR));
      return false;
    }

    AgentConfiguration aconf = agentConfigMap.get(agentName);

    if (aconf == null) {
      aconf = new AgentConfiguration(agentName, errors);
      agentConfigMap.put(agentName, aconf);
    }

    // Each configuration key must begin with one of the three prefixes:
    // sources, sinks, or channels.
    return aconf.addProperty(configKey, value);
  }

  public static class AgentConfiguration {

    private final String agentName;
    private String sources;
    private String sinks;
    private String channels;
    private String sinkgroups;

    private final Map<String, ComponentConfiguration> sourceConfigMap;
    private final Map<String, ComponentConfiguration> sinkConfigMap;
    private final Map<String, ComponentConfiguration> channelConfigMap;
    private final Map<String, ComponentConfiguration> sinkgroupConfigMap;

    private Map<String, Context> sourceContextMap;
    private Map<String, Context> sinkContextMap;
    private Map<String, Context> channelContextMap;
    private Map<String, Context> sinkGroupContextMap;

    private Set<String> sinkSet;
    private Set<String> sourceSet;
    private Set<String> channelSet;
    private Set<String> sinkgroupSet;

    private final List<FlumeConfigurationError> errorList;

    private AgentConfiguration(String agentName,
                               List<FlumeConfigurationError> errorList) {
      this.agentName = agentName;
      this.errorList = errorList;
      sourceConfigMap = new HashMap<String, ComponentConfiguration>();
      sinkConfigMap = new HashMap<String, ComponentConfiguration>();
      channelConfigMap = new HashMap<String, ComponentConfiguration>();
      sinkgroupConfigMap = new HashMap<String, ComponentConfiguration>();
      sourceContextMap = new HashMap<String, Context>();
      sinkContextMap = new HashMap<String, Context>();
      channelContextMap = new HashMap<String, Context>();
      sinkGroupContextMap = new HashMap<String, Context>();

    }

    public Map<String, ComponentConfiguration> getChannelConfigMap() {
      return channelConfigMap;
    }

    public Map<String, ComponentConfiguration> getSourceConfigMap() {
      return sourceConfigMap;
    }

    public Map<String, ComponentConfiguration> getSinkConfigMap() {
      return sinkConfigMap;
    }

    public Map<String, ComponentConfiguration> getSinkGroupConfigMap() {
      return sinkgroupConfigMap;
    }

    public Map<String, Context> getSourceContext() {
      return this.sourceContextMap;
    }

    public Map<String, Context> getSinkContext() {
      return this.sinkContextMap;
    }

    public Map<String, Context> getChannelContext() {
      return this.channelContextMap;
    }

    public Set<String> getSinkSet() {
      return sinkSet;
    }

    public Set<String> getSourceSet() {
      return sourceSet;
    }

    public Set<String> getChannelSet() {
      return channelSet;
    }

    public Set<String> getSinkgroupSet() {
      return sinkgroupSet;
    }


    /**
     * <p>
     * Checks the validity of the agent configuration. This method assumes that
     * all necessary configuration keys have been populated and are ready for
     * validation.
     * </p>
     * <p>
     * During the validation process, the components with invalid configuration
     * will be dropped. If at the end of this process, the minimum necessary
     * components are not available, the configuration itself will be considered
     * invalid.
     * </p>
     *
     * @return true if the configuration is valid, false otherwise
     */
    private boolean isValid() {
      logger.debug("Starting validation of configuration for agent: {}", agentName);
      if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
        logger.debug("Initial configuration: {}", this.getPrevalidationConfig());
      }

      // Make sure that at least one channel is specified
      if (channels == null || channels.trim().length() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' does not contain any channels. Marking it as invalid.");
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_CHANNELS,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.ERROR));
        return false;
      }

      channelSet =
          new HashSet<String>(Arrays
              .asList(channels.split("\\s+")));
      // validateComponent(channelSet, channelConfigMap, CLASS_CHANNEL,
      // ATTR_TYPE);

      channelSet = validateChannels(channelSet);
      if (channelSet.size() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' does not contain any valid channels. Marking it as invalid.");
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_CHANNELS,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.ERROR));
        return false;
      }

      sourceSet = validateSources(channelSet);
      sinkSet = validateSinks(channelSet);
      sinkgroupSet = validateGroups(sinkSet);

      // If no sources or sinks are present, then this is invalid
      if (sourceSet.size() == 0 && sinkSet.size() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' has no sources or sinks. Will be marked invalid.");
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_SOURCES,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.ERROR));
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_SINKS,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.ERROR));
        return false;
      }

      // Now rewrite the sources/sinks/channels

      this.sources = getSpaceDelimitedList(sourceSet);
      this.channels = getSpaceDelimitedList(channelSet);
      this.sinks = getSpaceDelimitedList(sinkSet);
      this.sinkgroups = getSpaceDelimitedList(sinkgroupSet);

      if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
        logger.debug("Post validation configuration for {}", agentName);
        logger.debug(this.getPostvalidationConfig());
      }

      return true;
    }

    private ChannelType getKnownChannel(String type) {
      ChannelType[] values = ChannelType.values();
      for (ChannelType value : values) {
        if (value.toString().equalsIgnoreCase(type)) return value;

        String channel = value.getChannelClassName();

        if (channel != null && channel.equalsIgnoreCase(type)) return value;

      }
      return null;
    }

    private SinkType getKnownSink(String type) {
      SinkType[] values = SinkType.values();
      for (SinkType value : values) {
        if (value.toString().equalsIgnoreCase(type)) return value;
        String sink = value.getSinkClassName();
        if (sink != null && sink.equalsIgnoreCase(type)) return value;
      }
      return null;
    }

    private SourceType getKnownSource(String type) {
      SourceType[] values = SourceType.values();
      for (SourceType value : values) {
        if (value.toString().equalsIgnoreCase(type)) return value;
        String src = value.getSourceClassName();
        if (src != null && src.equalsIgnoreCase(type)) return value;
      }
      return null;
    }

    /**
     * If it is a known component it will do the full validation required for
     * that component, else it will do the validation required for that class.
     */
    private Set<String> validateChannels(Set<String> channelSet) {
      Iterator<String> iter = channelSet.iterator();
      Map<String, Context> newContextMap = new HashMap<String, Context>();
      ChannelConfiguration conf = null;
      /*
       * The logic for the following code:
       *
       * Is it a known component?
       *  -Yes: Get the ChannelType and set the string name of that to
       *        config and set configSpecified to true.
       *  -No.Look for config type for the given component:
       *      -Config Found:
       *        Set config to the type mentioned, set configSpecified to true
       *      -No Config found:
       *        Set config to OTHER, configSpecified to false,
       *        do basic validation. Leave the context in the
       *        contextMap to process later. Setting it to other returns
       *        a vanilla configuration(Source/Sink/Channel Configuration),
       *        which does basic syntactic validation. This object is not
       *        put into the map, so the context is retained which can be
       *        picked up - this is meant for older classes which don't
       *        implement ConfigurableComponent.
       */
      while (iter.hasNext()) {
        String channelName = iter.next();
        Context channelContext = channelContextMap.get(channelName);
        // Context exists in map.
        if (channelContext != null) {
          // Get the configuration object for the channel:
          ChannelType chType = getKnownChannel(channelContext.getString(
              BasicConfigurationConstants.CONFIG_TYPE));
          boolean configSpecified = false;
          String config = null;
          // Not a known channel - cannot do specific validation to this channel
          if (chType == null) {
            config = channelContext.getString(BasicConfigurationConstants.CONFIG_CONFIG);
            if (config == null || config.isEmpty()) {
              config = "OTHER";
            } else {
              configSpecified = true;
            }
          } else {
            config = chType.toString().toUpperCase(Locale.ENGLISH);
            configSpecified = true;
          }

          try {
            conf =
                (ChannelConfiguration) ComponentConfigurationFactory.create(
                    channelName, config, ComponentType.CHANNEL);
            logger.debug("Created channel " + channelName);
            if (conf != null) {
              conf.configure(channelContext);
            }
            if ((configSpecified && conf.isNotFoundConfigClass()) ||
                !configSpecified) {
              newContextMap.put(channelName, channelContext);
            } else if (configSpecified) {
              channelConfigMap.put(channelName, conf);
            }
            if (conf != null) {
              errorList.addAll(conf.getErrors());
            }
          } catch (ConfigurationException e) {
            // Could not configure channel - skip it.
            // No need to add to error list - already added before exception is
            // thrown
            if (conf != null) errorList.addAll(conf.getErrors());
            iter.remove();
            logger.warn("Could not configure channel " + channelName
                + " due to: " + e.getMessage(), e);

          }
        } else {
          iter.remove();
          errorList.add(new FlumeConfigurationError(agentName, channelName,
              FlumeConfigurationErrorType.CONFIG_ERROR, ErrorOrWarning.ERROR));
        }
      }
      channelContextMap = newContextMap;
      Set<String> tempchannelSet = new HashSet<String>();
      tempchannelSet.addAll(channelConfigMap.keySet());
      tempchannelSet.addAll(channelContextMap.keySet());
      channelSet.retainAll(tempchannelSet);
      return channelSet;
    }

    private Set<String> validateSources(Set<String> channelSet) {
      //Arrays.split() call will throw NPE if the sources string is empty
      if (sources == null || sources.isEmpty()) {
        logger.warn("Agent configuration for '" + agentName
            + "' has no sources.");
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_SOURCES,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.WARNING));
        return new HashSet<String>();
      }
      Set<String> sourceSet =
          new HashSet<String>(Arrays.asList(sources.split("\\s+")));
      Map<String, Context> newContextMap = new HashMap<String, Context>();
      Iterator<String> iter = sourceSet.iterator();
      SourceConfiguration srcConf = null;
      /*
       * The logic for the following code:
       *
       * Is it a known component?
       *  -Yes: Get the SourceType and set the string name of that to
       *        config and set configSpecified to true.
       *  -No.Look for config type for the given component:
       *      -Config Found:
       *        Set config to the type mentioned, set configSpecified to true
       *      -No Config found:
       *        Set config to OTHER, configSpecified to false,
       *        do basic validation. Leave the context in the
       *        contextMap to process later. Setting it to other returns
       *        a vanilla configuration(Source/Sink/Channel Configuration),
       *        which does basic syntactic validation. This object is not
       *        put into the map, so the context is retained which can be
       *        picked up - this is meant for older classes which don't
       *        implement ConfigurableComponent.
       */
      while (iter.hasNext()) {
        String sourceName = iter.next();
        Context srcContext = sourceContextMap.get(sourceName);
        String config = null;
        boolean configSpecified = false;
        if (srcContext != null) {
          SourceType srcType = getKnownSource(srcContext.getString(
              BasicConfigurationConstants.CONFIG_TYPE));
          if (srcType == null) {
            config = srcContext.getString(
                BasicConfigurationConstants.CONFIG_CONFIG);
            if (config == null || config.isEmpty()) {
              config = "OTHER";
            } else {
              configSpecified = true;
            }
          } else {
            config = srcType.toString().toUpperCase(Locale.ENGLISH);
            configSpecified = true;
          }
          try {
            // Possible reason the configuration can fail here:
            // Old component is configured directly using Context
            srcConf =
                (SourceConfiguration) ComponentConfigurationFactory.create(
                    sourceName, config, ComponentType.SOURCE);
            if (srcConf != null) {
              srcConf.configure(srcContext);
              Set<String> channels = new HashSet<String>();
              if (srcConf.getChannels() != null) {
                channels.addAll(srcConf.getChannels());
              }
              channels.retainAll(channelSet);
              if (channels.isEmpty()) {
                throw new ConfigurationException(
                    "No Channels configured for " + sourceName);
              }
              srcContext.put(BasicConfigurationConstants.CONFIG_CHANNELS,
                  this.getSpaceDelimitedList(channels));
            }
            if ((configSpecified && srcConf.isNotFoundConfigClass()) ||
                !configSpecified) {
              newContextMap.put(sourceName, srcContext);
            } else if (configSpecified) {
              sourceConfigMap.put(sourceName, srcConf);
            }
            if (srcConf != null) errorList.addAll(srcConf.getErrors());
          } catch (ConfigurationException e) {
            if (srcConf != null) errorList.addAll(srcConf.getErrors());
            iter.remove();
            logger.warn("Could not configure source  " + sourceName
                + " due to: " + e.getMessage(), e);
          }
        } else {
          iter.remove();
          errorList.add(new FlumeConfigurationError(agentName, sourceName,
              FlumeConfigurationErrorType.CONFIG_ERROR, ErrorOrWarning.ERROR));
          logger.warn("Configuration empty for: " + sourceName + ".Removed.");
        }
      }

      // validateComponent(sourceSet, sourceConfigMap, CLASS_SOURCE, ATTR_TYPE,
      // ATTR_CHANNELS);
      sourceContextMap = newContextMap;
      Set<String> tempsourceSet = new HashSet<String>();
      tempsourceSet.addAll(sourceContextMap.keySet());
      tempsourceSet.addAll(sourceConfigMap.keySet());
      sourceSet.retainAll(tempsourceSet);
      return sourceSet;
    }

    private Set<String> validateSinks(Set<String> channelSet) {
      // Preconditions.checkArgument(channelSet != null && channelSet.size() >
      // 0);
      Map<String, Context> newContextMap = new HashMap<String, Context>();
      Set<String> sinkSet;
      SinkConfiguration sinkConf = null;
      if (sinks == null || sinks.isEmpty()) {
        logger.warn("Agent configuration for '" + agentName
            + "' has no sinks.");
        errorList.add(new FlumeConfigurationError(agentName,
            BasicConfigurationConstants.CONFIG_SINKS,
            FlumeConfigurationErrorType.PROPERTY_VALUE_NULL,
            ErrorOrWarning.WARNING));
        return new HashSet<String>();
      } else {
        sinkSet =
            new HashSet<String>(Arrays.asList(sinks.split("\\s+")));
      }
      Iterator<String> iter = sinkSet.iterator();
      /*
       * The logic for the following code:
       *
       * Is it a known component?
       *  -Yes: Get the SinkType and set the string name of that to
       *        config and set configSpecified to true.
       *  -No.Look for config type for the given component:
       *      -Config Found:
       *        Set config to the type mentioned, set configSpecified to true
       *      -No Config found:
       *        Set config to OTHER, configSpecified to false,
       *        do basic validation. Leave the context in the
       *        contextMap to process later. Setting it to other returns
       *        a vanilla configuration(Source/Sink/Channel Configuration),
       *        which does basic syntactic validation. This object is not
       *        put into the map, so the context is retained which can be
       *        picked up - this is meant for older classes which don't
       *        implement ConfigurableComponent.
       */
      while (iter.hasNext()) {
        String sinkName = iter.next();
        Context sinkContext = sinkContextMap.get(sinkName.trim());
        if (sinkContext == null) {
          iter.remove();
          logger.warn("no context for sink" + sinkName);
          errorList.add(new FlumeConfigurationError(agentName, sinkName,
              FlumeConfigurationErrorType.CONFIG_ERROR, ErrorOrWarning.ERROR));
        } else {
          String config = null;
          boolean configSpecified = false;
          SinkType sinkType = getKnownSink(sinkContext.getString(
              BasicConfigurationConstants.CONFIG_TYPE));
          if (sinkType == null) {
            config = sinkContext.getString(
                BasicConfigurationConstants.CONFIG_CONFIG);
            if (config == null || config.isEmpty()) {
              config = "OTHER";
            } else {
              configSpecified = true;
            }
          } else {
            config = sinkType.toString().toUpperCase(Locale.ENGLISH);
            configSpecified = true;
          }
          try {
            logger.debug("Creating sink: " + sinkName + " using " + config);

            sinkConf =
                (SinkConfiguration) ComponentConfigurationFactory.create(
                    sinkName, config, ComponentType.SINK);
            if (sinkConf != null) {
              sinkConf.configure(sinkContext);

            }
            if (!channelSet.contains(sinkConf.getChannel())) {
              throw new ConfigurationException("Channel " +
                  sinkConf.getChannel() + " not in active set.");
            }
            if ((configSpecified && sinkConf.isNotFoundConfigClass()) ||
                !configSpecified) {
              newContextMap.put(sinkName, sinkContext);
            } else if (configSpecified) {
              sinkConfigMap.put(sinkName, sinkConf);
            }
            if (sinkConf != null) errorList.addAll(sinkConf.getErrors());
          } catch (ConfigurationException e) {
            iter.remove();
            if (sinkConf != null) errorList.addAll(sinkConf.getErrors());
            logger.warn("Could not configure sink  " + sinkName
                + " due to: " + e.getMessage(), e);
          }
        }
        // Filter out any sinks that have invalid channel

      }
      sinkContextMap = newContextMap;
      Set<String> tempSinkset = new HashSet<String>();
      tempSinkset.addAll(sinkConfigMap.keySet());
      tempSinkset.addAll(sinkContextMap.keySet());
      sinkSet.retainAll(tempSinkset);
      return sinkSet;

      // validateComponent(sinkSet, sinkConfigMap, CLASS_SINK, ATTR_TYPE,
      // ATTR_CHANNEL);
    }

    /**
     * Validates that each group has at least one sink, blocking other groups
     * from acquiring it
     *
     * @param sinkSet
     *          Set of valid sinks
     * @return Set of valid sinkgroups
     */
    private Set<String> validateGroups(Set<String> sinkSet) {
      Set<String> sinkgroupSet = stringToSet(sinkgroups, " ");
      Map<String, String> usedSinks = new HashMap<String, String>();
      Iterator<String> iter = sinkgroupSet.iterator();
      SinkGroupConfiguration conf;

      while (iter.hasNext()) {
        String sinkgroupName = iter.next();
        Context context = this.sinkGroupContextMap.get(sinkgroupName);
        if (context != null) {
          try {
            conf =
                (SinkGroupConfiguration) ComponentConfigurationFactory.create(
                    sinkgroupName, "sinkgroup", ComponentType.SINKGROUP);

            conf.configure(context);
            Set<String> groupSinks = validGroupSinks(sinkSet, usedSinks, conf);
            if (conf != null) errorList.addAll(conf.getErrors());
            if (groupSinks != null && !groupSinks.isEmpty()) {
              List<String> sinkArray = new ArrayList<String>();
              for (String sink : groupSinks) {
                sinkArray.add(sink);
              }
              conf.setSinks(sinkArray);
              sinkgroupConfigMap.put(sinkgroupName, conf);
            } else {
              errorList.add(new FlumeConfigurationError(agentName, sinkgroupName,
                  FlumeConfigurationErrorType.CONFIG_ERROR,
                  ErrorOrWarning.ERROR));
              if (conf != null) errorList.addAll(conf.getErrors());
              throw new ConfigurationException(
                  "No available sinks for sinkgroup: " + sinkgroupName
                      + ". Sinkgroup will be removed");
            }

          } catch (ConfigurationException e) {
            iter.remove();
            errorList
                .add(new FlumeConfigurationError(agentName, sinkgroupName,
                    FlumeConfigurationErrorType.CONFIG_ERROR,
                    ErrorOrWarning.ERROR));
            logger.warn("Could not configure sink group " + sinkgroupName
                + " due to: " + e.getMessage(), e);
          }
        } else {
          iter.remove();
          errorList.add(new FlumeConfigurationError(agentName, sinkgroupName,
              FlumeConfigurationErrorType.CONFIG_ERROR, ErrorOrWarning.ERROR));
          logger.warn("Configuration error for: " + sinkgroupName
              + ".Removed.");
        }

      }

      sinkgroupSet.retainAll(sinkgroupConfigMap.keySet());
      return sinkgroupSet;
    }

    /**
     * Check availability of sinks for group
     *
     * @param sinkSet
     *          [in]Existing valid sinks
     * @param usedSinks
     *          [in/out]Sinks already in use by other groups
     * @param groupConf
     *          [in]sinkgroup configuration
     * @return List of sinks available and reserved for group
     */
    private Set<String> validGroupSinks(Set<String> sinkSet,
                                        Map<String, String> usedSinks,
                                        SinkGroupConfiguration groupConf) {
      Set<String> groupSinks =
          Collections.synchronizedSet(new HashSet<String>(groupConf.getSinks()));

      if (groupSinks.isEmpty()) return null;
      Iterator<String> sinkIt = groupSinks.iterator();
      while (sinkIt.hasNext()) {
        String curSink = sinkIt.next();
        if (usedSinks.containsKey(curSink)) {
          logger.warn("Agent configuration for '" + agentName + "' sinkgroup '"
              + groupConf.getComponentName() + "' sink '" + curSink
              + "' in use by " + "another group: '" + usedSinks.get(curSink)
              + "', sink not added");
          errorList.add(new FlumeConfigurationError(agentName, groupConf
              .getComponentName(),
              FlumeConfigurationErrorType.PROPERTY_PART_OF_ANOTHER_GROUP,
              ErrorOrWarning.ERROR));
          sinkIt.remove();
          continue;
        } else if (!sinkSet.contains(curSink)) {
          logger.warn("Agent configuration for '" + agentName + "' sinkgroup '"
              + groupConf.getComponentName() + "' sink not found: '" + curSink
              + "',  sink not added");
          errorList.add(new FlumeConfigurationError(agentName, curSink,
              FlumeConfigurationErrorType.INVALID_PROPERTY,
              ErrorOrWarning.ERROR));
          sinkIt.remove();
          continue;
        } else {
          usedSinks.put(curSink, groupConf.getComponentName());
        }
      }
      return groupSinks;
    }

    private String getSpaceDelimitedList(Set<String> entries) {
      if (entries.size() == 0) {
        return null;
      }

      StringBuilder sb = new StringBuilder("");

      for (String entry : entries) {
        sb.append(" ").append(entry);
      }

      return sb.toString().trim();
    }

    private static Set<String> stringToSet(String target, String delim) {
      Set<String> out = new HashSet<String>();
      if (target == null || target.trim().length() == 0) {
        return out;
      }
      StringTokenizer t = new StringTokenizer(target, delim);
      while (t.hasMoreTokens()) {
        out.add(t.nextToken());
      }
      return out;
    }

    public String getPrevalidationConfig() {
      StringBuilder sb = new StringBuilder("AgentConfiguration[");
      sb.append(agentName).append("]").append(NEWLINE).append("SOURCES: ");
      sb.append(sourceContextMap).append(NEWLINE).append("CHANNELS: ");
      sb.append(channelContextMap).append(NEWLINE).append("SINKS: ");
      sb.append(sinkContextMap).append(NEWLINE);

      return sb.toString();
    }

    public String getPostvalidationConfig() {
      StringBuilder sb = new StringBuilder(
          "AgentConfiguration created without Configuration stubs " +
              "for which only basic syntactical validation was performed[");
      sb.append(agentName).append("]").append(NEWLINE);
      if (!sourceContextMap.isEmpty() ||
          !sinkContextMap.isEmpty() ||
          !channelContextMap.isEmpty()) {
        if (!sourceContextMap.isEmpty()) {
          sb.append("SOURCES: ").append(sourceContextMap).append(NEWLINE);
        }

        if (!channelContextMap.isEmpty()) {
          sb.append("CHANNELS: ").append(channelContextMap).append(NEWLINE);
        }

        if (!sinkContextMap.isEmpty()) {
          sb.append("SINKS: ").append(sinkContextMap).append(NEWLINE);
        }
      }

      if (!sourceConfigMap.isEmpty() ||
          !sinkConfigMap.isEmpty() ||
          !channelConfigMap.isEmpty()) {
        sb.append("AgentConfiguration created with Configuration stubs " +
            "for which full validation was performed[");
        sb.append(agentName).append("]").append(NEWLINE);

        if (!sourceConfigMap.isEmpty()) {
          sb.append("SOURCES: ").append(sourceConfigMap).append(NEWLINE);
        }

        if (!channelConfigMap.isEmpty()) {
          sb.append("CHANNELS: ").append(channelConfigMap).append(NEWLINE);
        }

        if (!sinkConfigMap.isEmpty()) {
          sb.append("SINKS: ").append(sinkConfigMap).append(NEWLINE);
        }
      }

      return sb.toString();
    }

    private boolean addProperty(String key, String value) {
      // Check for sources
      if (key.equals(BasicConfigurationConstants.CONFIG_SOURCES)) {
        if (sources == null) {
          sources = value;
          return true;
        } else {
          logger
              .warn("Duplicate source list specified for agent: " + agentName);
          errorList.add(new FlumeConfigurationError(agentName,
              BasicConfigurationConstants.CONFIG_SOURCES,
              FlumeConfigurationErrorType.DUPLICATE_PROPERTY,
              ErrorOrWarning.ERROR));
          return false;
        }
      }

      // Check for sinks
      if (key.equals(BasicConfigurationConstants.CONFIG_SINKS)) {
        if (sinks == null) {
          sinks = value;
          logger.info("Added sinks: " + sinks + " Agent: " + this.agentName);
          return true;
        } else {
          logger.warn("Duplicate sink list specfied for agent: " + agentName);
          errorList.add(new FlumeConfigurationError(agentName,
              BasicConfigurationConstants.CONFIG_SINKS,
              FlumeConfigurationErrorType.DUPLICATE_PROPERTY,
              ErrorOrWarning.ERROR));
          return false;
        }
      }

      // Check for channels
      if (key.equals(BasicConfigurationConstants.CONFIG_CHANNELS)) {
        if (channels == null) {
          channels = value;

          return true;
        } else {
          logger.warn("Duplicate channel list specified for agent: "
              + agentName);
          errorList.add(new FlumeConfigurationError(agentName,
              BasicConfigurationConstants.CONFIG_CHANNELS,
              FlumeConfigurationErrorType.DUPLICATE_PROPERTY,
              ErrorOrWarning.ERROR));
          return false;
        }
      }

      // Check for sinkgroups
      if (key.equals(BasicConfigurationConstants.CONFIG_SINKGROUPS)) {
        if (sinkgroups == null) {
          sinkgroups = value;

          return true;
        } else {
          logger
              .warn("Duplicate sinkgroup list specfied for agent: " + agentName);
          errorList.add(new FlumeConfigurationError(agentName,
              BasicConfigurationConstants.CONFIG_SINKGROUPS,
              FlumeConfigurationErrorType.DUPLICATE_PROPERTY,
              ErrorOrWarning.ERROR));
          return false;
        }
      }

      ComponentNameAndConfigKey cnck = parseConfigKey(key,
          BasicConfigurationConstants.CONFIG_SOURCES_PREFIX);

      if (cnck != null) {
        // it is a source
        String name = cnck.getComponentName();
        Context srcConf = sourceContextMap.get(name);

        if (srcConf == null) {
          srcConf = new Context();
          sourceContextMap.put(name, srcConf);
        }

        srcConf.put(cnck.getConfigKey(), value);
        return true;
      }

      cnck = parseConfigKey(key,
          BasicConfigurationConstants.CONFIG_CHANNELS_PREFIX);

      if (cnck != null) {
        // it is a channel
        String name = cnck.getComponentName();
        Context channelConf = channelContextMap.get(name);

        if (channelConf == null) {
          channelConf = new Context();
          channelContextMap.put(name, channelConf);
        }

        channelConf.put(cnck.getConfigKey(), value);
        return true;
      }

      cnck = parseConfigKey(key,
          BasicConfigurationConstants.CONFIG_SINKS_PREFIX);

      if (cnck != null) {
        // it is a sink
        String name = cnck.getComponentName().trim();
        logger.info("Processing:" + name);
        Context sinkConf = sinkContextMap.get(name);

        if (sinkConf == null) {
          logger.debug("Created context for " + name + ": "
              + cnck.getConfigKey());
          sinkConf = new Context();
          sinkContextMap.put(name, sinkConf);
        }

        sinkConf.put(cnck.getConfigKey(), value);
        return true;
      }

      cnck = parseConfigKey(key,
          BasicConfigurationConstants.CONFIG_SINKGROUPS_PREFIX);

      if (cnck != null) {
        String name = cnck.getComponentName();
        Context groupConf = sinkGroupContextMap.get(name);
        if (groupConf == null) {
          groupConf = new Context();
          sinkGroupContextMap.put(name, groupConf);
        }

        groupConf.put(cnck.getConfigKey(), value);

        return true;
      }

      logger.warn("Invalid property specified: " + key);
      errorList.add(new FlumeConfigurationError(agentName, key,
          FlumeConfigurationErrorType.INVALID_PROPERTY, ErrorOrWarning.ERROR));
      return false;
    }

    private ComponentNameAndConfigKey parseConfigKey(String key, String prefix) {
      // key must start with prefix
      if (!key.startsWith(prefix)) {
        return null;
      }

      // key must have a component name part after the prefix of the format:
      // <prefix><component-name>.<config-key>
      int index = key.indexOf('.', prefix.length() + 1);

      if (index == -1) {
        return null;
      }

      String name = key.substring(prefix.length(), index);
      String configKey = key.substring(prefix.length() + name.length() + 1);

      // name and config key must be non-empty
      if (name.length() == 0 || configKey.length() == 0) {
        return null;
      }

      return new ComponentNameAndConfigKey(name, configKey);
    }
  }

  public static class ComponentNameAndConfigKey {

    private final String componentName;
    private final String configKey;

    private ComponentNameAndConfigKey(String name, String configKey) {
      this.componentName = name;
      this.configKey = configKey;
    }

    public String getComponentName() {
      return componentName;
    }

    public String getConfigKey() {
      return configKey;
    }
  }
}
