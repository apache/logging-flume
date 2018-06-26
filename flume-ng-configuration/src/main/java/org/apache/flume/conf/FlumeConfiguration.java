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
import org.apache.flume.configfilter.ConfigFilter;
import org.apache.flume.conf.configfilter.ConfigFilterConfiguration;
import org.apache.flume.conf.configfilter.ConfigFilterType;
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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CHANNELS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CHANNELS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIG;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIGFILTERS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIGFILTERS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKGROUPS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKGROUPS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SOURCES;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SOURCES_PREFIX;
import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.ERROR;
import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.WARNING;
import static org.apache.flume.conf.FlumeConfigurationErrorType.AGENT_CONFIGURATION_INVALID;
import static org.apache.flume.conf.FlumeConfigurationErrorType.AGENT_NAME_MISSING;
import static org.apache.flume.conf.FlumeConfigurationErrorType.CONFIG_ERROR;
import static org.apache.flume.conf.FlumeConfigurationErrorType.DUPLICATE_PROPERTY;
import static org.apache.flume.conf.FlumeConfigurationErrorType.INVALID_PROPERTY;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_NAME_NULL;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_PART_OF_ANOTHER_GROUP;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_VALUE_NULL;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(FlumeConfiguration.class);

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
    agentConfigMap = new HashMap<>();
    errors = new LinkedList<>();
    // Construct the in-memory component hierarchy
    for (Entry entry : properties.entrySet()) {
      if (!addRawProperty(entry.getKey().toString(), entry.getValue().toString())) {
        LOGGER.warn("Configuration property ignored: {} = {}", entry.getKey(), entry.getValue());
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
    agentConfigMap = new HashMap<>();
    errors = new LinkedList<>();
    // Construct the in-memory component hierarchy
    for (Entry<String, String> entry : properties.entrySet()) {
      if (!addRawProperty(entry.getKey(), entry.getValue())) {
        LOGGER.warn("Configuration property ignored: {} = {}", entry.getKey(), entry.getValue());
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
    Set<Entry<String, AgentConfiguration>> entries = agentConfigMap.entrySet();
    Iterator<Entry<String, AgentConfiguration>> it = entries.iterator();

    while (it.hasNext()) {
      Entry<String, AgentConfiguration> next = it.next();
      String agentName = next.getKey();
      AgentConfiguration aconf = next.getValue();

      if (!aconf.isValid()) {
        LOGGER.warn("Agent configuration invalid for agent '{}'. It will be removed.", agentName);
        addError(agentName, AGENT_CONFIGURATION_INVALID, ERROR);
        it.remove();
      }
      LOGGER.debug("Channels:{}\n", aconf.channels);
      LOGGER.debug("Sinks {}\n", aconf.sinks);
      LOGGER.debug("Sources {}\n", aconf.sources);
    }

    LOGGER.info(
        "Post-validation flume configuration contains configuration for agents: {}",
        agentConfigMap.keySet()
    );
  }

  private boolean addRawProperty(String rawName, String rawValue) {
    // Null names and values not supported
    if (rawName == null || rawValue == null) {
      addError("", AGENT_NAME_MISSING, ERROR);
      return false;
    }

    // Remove leading and trailing spaces
    String name = rawName.trim();
    String value = rawValue.trim();

    // Empty values are not supported
    if (value.isEmpty()) {
      addError(name, PROPERTY_VALUE_NULL, ERROR);
      return false;
    }

    int index = name.indexOf('.');

    // All configuration keys must have a prefix defined as agent name
    if (index == -1) {
      addError(name, AGENT_NAME_MISSING, ERROR);
      return false;
    }

    String agentName = name.substring(0, index);

    // Agent name must be specified for all properties
    if (agentName.isEmpty()) {
      addError(name, AGENT_NAME_MISSING, ERROR);
      return false;
    }

    String configKey = name.substring(index + 1);

    // Configuration key must be specified for every property
    if (configKey.isEmpty()) {
      addError(name, PROPERTY_NAME_NULL, ERROR);
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

  private void addError(
      String component, FlumeConfigurationErrorType errorType, ErrorOrWarning level
  ) {
    errors.add(new FlumeConfigurationError(component, "", errorType, level));
  }

  public static class AgentConfiguration {

    private final String agentName;
    private String configFilters;
    private String sources;
    private String sinks;
    private String channels;
    private String sinkgroups;

    private final Map<String, ComponentConfiguration> sourceConfigMap;
    private final Map<String, ComponentConfiguration> sinkConfigMap;
    private final Map<String, ComponentConfiguration> channelConfigMap;
    private final Map<String, ComponentConfiguration> sinkgroupConfigMap;
    private final Map<String, ComponentConfiguration> configFilterConfigMap;

    private Map<String, Context> configFilterContextMap;
    private Map<String, Context> sourceContextMap;
    private Map<String, Context> sinkContextMap;
    private Map<String, Context> channelContextMap;
    private Map<String, Context> sinkGroupContextMap;

    private Set<String> sinkSet;
    private Set<String> configFilterSet;
    private Set<String> sourceSet;
    private Set<String> channelSet;
    private Set<String> sinkgroupSet;

    private final List<FlumeConfigurationError> errorList;
    private List<ConfigFilter> configFiltersInstances;
    private Map<String, Pattern> configFilterPatternCache;

    private AgentConfiguration(String agentName,
                               List<FlumeConfigurationError> errorList) {
      this.agentName = agentName;
      this.errorList = errorList;
      configFilterConfigMap = new HashMap<>();
      sourceConfigMap = new HashMap<>();
      sinkConfigMap = new HashMap<>();
      channelConfigMap = new HashMap<>();
      sinkgroupConfigMap = new HashMap<>();
      configFilterContextMap = new HashMap<>();
      sourceContextMap = new HashMap<>();
      sinkContextMap = new HashMap<>();
      channelContextMap = new HashMap<>();
      sinkGroupContextMap = new HashMap<>();
      configFiltersInstances = new ArrayList<>();
      configFilterPatternCache = new HashMap<>();
    }

    public Map<String, ComponentConfiguration> getChannelConfigMap() {
      return channelConfigMap;
    }

    public Map<String, ComponentConfiguration> getSourceConfigMap() {
      return sourceConfigMap;
    }

    public Map<String, ComponentConfiguration> getConfigFilterConfigMap() {
      return configFilterConfigMap;
    }

    public Map<String, ComponentConfiguration> getSinkConfigMap() {
      return sinkConfigMap;
    }

    public Map<String, ComponentConfiguration> getSinkGroupConfigMap() {
      return sinkgroupConfigMap;
    }

    public Map<String, Context> getConfigFilterContext() {
      return configFilterContextMap;
    }

    public Map<String, Context> getSourceContext() {
      return sourceContextMap;
    }

    public Map<String, Context> getSinkContext() {
      return sinkContextMap;
    }

    public Map<String, Context> getChannelContext() {
      return channelContextMap;
    }

    public Set<String> getSinkSet() {
      return sinkSet;
    }

    public Set<String> getConfigFilterSet() {
      return configFilterSet;
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
      LOGGER.debug("Starting validation of configuration for agent: {}", agentName);
      if (LOGGER.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
        LOGGER.debug("Initial configuration: {}", getPrevalidationConfig());
      }

      configFilterSet = validateConfigFilterSet();
      createConfigFilters();
      runFiltersThroughConfigs();

      // Make sure that at least one channel is specified
      if (channels == null || channels.trim().isEmpty()) {
        LOGGER.warn(
            "Agent configuration for '{}' does not contain any channels. Marking it as invalid.",
            agentName
        );
        addError(CONFIG_CHANNELS, PROPERTY_VALUE_NULL, ERROR);
        return false;
      }

      channelSet = new HashSet<>(Arrays.asList(channels.split("\\s+")));

      channelSet = validateChannels(channelSet);
      if (channelSet.isEmpty()) {
        LOGGER.warn(
            "Agent configuration for '{}' does not contain any valid channels. " +
                "Marking it as invalid.",
            agentName
        );
        addError(CONFIG_CHANNELS, PROPERTY_VALUE_NULL, ERROR);
        return false;
      }

      sourceSet = validateSources(channelSet);
      sinkSet = validateSinks(channelSet);
      sinkgroupSet = validateGroups(sinkSet);

      // If no sources or sinks are present, then this is invalid
      if (sourceSet.isEmpty() && sinkSet.isEmpty()) {
        LOGGER.warn(
            "Agent configuration for '{}' has no sources or sinks. Will be marked invalid.",
            agentName
        );
        addError(CONFIG_SOURCES, PROPERTY_VALUE_NULL, ERROR);
        addError(CONFIG_SINKS, PROPERTY_VALUE_NULL, ERROR);
        return false;
      }

      // Now rewrite the sources/sinks/channels

      this.configFilters = getSpaceDelimitedList(configFilterSet);
      sources = getSpaceDelimitedList(sourceSet);
      channels = getSpaceDelimitedList(channelSet);
      sinks = getSpaceDelimitedList(sinkSet);
      sinkgroups = getSpaceDelimitedList(sinkgroupSet);

      if (LOGGER.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
        LOGGER.debug("Post validation configuration for {}", agentName);
        LOGGER.debug(getPostvalidationConfig());
      }

      return true;
    }

    private void runFiltersThroughConfigs() {
      runFiltersOnContextMaps(
          sourceContextMap,
          channelContextMap,
          sinkContextMap,
          sinkGroupContextMap
      );
    }

    private void runFiltersOnContextMaps(Map<String, Context>... maps) {
      for (Map<String, Context> map: maps) {
        for (Context context : map.values()) {
          for (String key : context.getParameters().keySet()) {
            filterValue(context, key);
          }
        }
      }
    }

    private void createConfigFilters() {
      for (String name: configFilterSet) {
        Context context = configFilterContextMap.get(name);
        ComponentConfiguration componentConfiguration = configFilterConfigMap.get(name);
        try {
          if (context != null) {
            ConfigFilter configFilter = ConfigFilterFactory.create(
                name, context.getString(BasicConfigurationConstants.CONFIG_TYPE)
            );
            configFilter.initializeWithConfiguration(context.getParameters());
            configFiltersInstances.add(configFilter);
            configFilterPatternCache.put(configFilter.getName(),
                createConfigFilterPattern(configFilter));
          } else if (componentConfiguration != null) {
            ConfigFilter configFilter = ConfigFilterFactory.create(
                componentConfiguration.getComponentName(), componentConfiguration.getType()
            );
            configFiltersInstances.add(configFilter);
            configFilterPatternCache.put(configFilter.getName(), 
                createConfigFilterPattern(configFilter));
          }
        } catch (Exception e) {
          LOGGER.error("Error while creating config filter {}", name, e);
        }
      }
    }

    private Pattern createConfigFilterPattern(ConfigFilter configFilter) {
      //JAVA EL expression style ${myFilterName['my_key']} or
      //JAVA EL expression style ${myFilterName["my_key"]} or
      //JAVA EL expression style ${myFilterName[my_key]}
      return Pattern.compile(
          "\\$\\{" +  // ${
              Pattern.quote(configFilter.getName()) + //<filterComponentName>
              "\\[(|'|\")" +  // delimiter :'," or nothing
              "(?<key>[-_a-zA-Z0-9]+)" + // key
              "\\1\\]" + // matching delimiter
              "\\}" // }
      );
    }

    private void filterValue(Context c, String contextKey) {
      for (ConfigFilter configFilter : configFiltersInstances) {
        try {
          Pattern pattern = configFilterPatternCache.get(configFilter.getName());
          String currentValue = c.getString(contextKey);
          Matcher matcher = pattern.matcher(currentValue);
          String filteredValue = currentValue;
          while (matcher.find()) {
            String key = matcher.group("key");
            LOGGER.debug("Replacing {} from config filter {}", key, configFilter.getName());
            String filtered = configFilter.filter(key);
            if (filtered == null) {
              continue;
            }
            String fullMatch = matcher.group();
            filteredValue = filteredValue.replace(fullMatch, filtered);
          }
          c.put(contextKey, filteredValue);
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("Error while matching and filtering configFilter: {} and key: {}",
              new Object[]{configFilter.getName(), contextKey, e});
        }
      }
    }

    private void addError(String key, FlumeConfigurationErrorType errorType, ErrorOrWarning level) {
      errorList.add(new FlumeConfigurationError(agentName, key, errorType, level));
    }

    private ChannelType getKnownChannel(String type) {
      return getKnownComponent(type, ChannelType.values());
    }

    private SinkType getKnownSink(String type) {
      return getKnownComponent(type, SinkType.values());
    }

    private SourceType getKnownSource(String type) {
      return getKnownComponent(type, SourceType.values());
    }

    private ConfigFilterType getKnownConfigFilter(String type) {
      return getKnownComponent(type, ConfigFilterType.values());
    }

    private <T extends ComponentWithClassName> T getKnownComponent(String type, T[] values) {
      for (T value : values) {
        if (value.toString().equalsIgnoreCase(type)) return value;
        String src = value.getClassName();
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
      Map<String, Context> newContextMap = new HashMap<>();
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
            config = channelContext.getString(CONFIG_CONFIG);
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
            LOGGER.debug("Created channel {}", channelName);
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
            LOGGER.warn(
                "Could not configure channel {} due to: {}",
                new Object[]{channelName, e.getMessage(), e}
            );

          }
        } else {
          iter.remove();
          addError(channelName, CONFIG_ERROR, ERROR);
        }
      }
      channelContextMap = newContextMap;
      Set<String> tempchannelSet = new HashSet<String>();
      tempchannelSet.addAll(channelConfigMap.keySet());
      tempchannelSet.addAll(channelContextMap.keySet());
      channelSet.retainAll(tempchannelSet);
      return channelSet;
    }

    private Set<String> validateConfigFilterSet() {
      if (configFilters == null || configFilters.isEmpty()) {
        LOGGER.warn("Agent configuration for '{}' has no configfilters.", agentName);
        return new HashSet<>();
      }
      Set<String> configFilterSet = new HashSet<>(Arrays.asList(configFilters.split("\\s+")));
      Map<String, Context> newContextMap = new HashMap<>();

      Iterator<String> iter = configFilterSet.iterator();
      ConfigFilterConfiguration conf = null;
      while (iter.hasNext()) {
        String configFilterName = iter.next();
        Context configFilterContext = configFilterContextMap.get(configFilterName);
        if (configFilterContext != null) {


          // Get the configuration object for the channel:
          ConfigFilterType chType = getKnownConfigFilter(configFilterContext.getString(
              BasicConfigurationConstants.CONFIG_TYPE));
          boolean configSpecified = false;
          String config = null;
          // Not a known channel - cannot do specific validation to this channel
          if (chType == null) {
            config = configFilterContext.getString(CONFIG_CONFIG);
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
                (ConfigFilterConfiguration) ComponentConfigurationFactory.create(
                    configFilterName, config, ComponentType.CONFIG_FILTER);
            LOGGER.debug("Created configfilter {}", configFilterName);
            if (conf != null) {
              conf.configure(configFilterContext);
            }
            if ((configSpecified && conf.isNotFoundConfigClass()) ||
                !configSpecified) {
              newContextMap.put(configFilterName, configFilterContext);
            } else if (configSpecified) {
              configFilterConfigMap.put(configFilterName, conf);
            }

            if (conf != null) {
              errorList.addAll(conf.getErrors());
            }
          } catch (ConfigurationException e) {
            if (conf != null) errorList.addAll(conf.getErrors());
            iter.remove();
            LOGGER.warn(
                "Could not configure configfilter {} due to: {}",
                new Object[]{configFilterName, e.getMessage(), e}
            );

          }
        } else {
          iter.remove();
          addError(configFilterName, CONFIG_ERROR, ERROR);
          LOGGER.warn("Configuration empty for: {}. Removed.", configFilterName);
        }
      }

      configFilterContextMap = newContextMap;
      Set<String> tempchannelSet = new HashSet<String>();
      tempchannelSet.addAll(configFilterConfigMap.keySet());
      tempchannelSet.addAll(configFilterContextMap.keySet());
      configFilterSet.retainAll(tempchannelSet);

      return configFilterSet;
    }


    private Set<String> validateSources(Set<String> channelSet) {
      //Arrays.split() call will throw NPE if the sources string is empty
      if (sources == null || sources.isEmpty()) {
        LOGGER.warn("Agent configuration for '{}' has no sources.", agentName);
        addError(CONFIG_SOURCES, PROPERTY_VALUE_NULL, WARNING);
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
                CONFIG_CONFIG);
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
              srcContext.put(CONFIG_CHANNELS,
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
            LOGGER.warn(
                "Could not configure source  {} due to: {}",
                new Object[]{sourceName, e.getMessage(), e}
            );
          }
        } else {
          iter.remove();
          addError(sourceName, CONFIG_ERROR, ERROR);
          LOGGER.warn("Configuration empty for: {}.Removed.", sourceName);
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
        LOGGER.warn("Agent configuration for '{}' has no sinks.", agentName);
        addError(CONFIG_SINKS, PROPERTY_VALUE_NULL, WARNING);
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
          LOGGER.warn("no context for sink{}", sinkName);
          addError(sinkName, CONFIG_ERROR, ERROR);
        } else {
          String config = null;
          boolean configSpecified = false;
          SinkType sinkType = getKnownSink(sinkContext.getString(
              BasicConfigurationConstants.CONFIG_TYPE));
          if (sinkType == null) {
            config = sinkContext.getString(
                CONFIG_CONFIG);
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
            LOGGER.debug("Creating sink: {} using {}", sinkName, config);

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
            LOGGER.warn(
                "Could not configure sink  {} due to: {}",
                new Object[]{sinkName, e.getMessage(), e}
            );
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
              sinkArray.addAll(groupSinks);
              conf.setSinks(sinkArray);
              sinkgroupConfigMap.put(sinkgroupName, conf);
            } else {
              addError(sinkgroupName, CONFIG_ERROR, ERROR);
              if (conf != null) errorList.addAll(conf.getErrors());
              throw new ConfigurationException(
                  "No available sinks for sinkgroup: " + sinkgroupName
                      + ". Sinkgroup will be removed");
            }

          } catch (ConfigurationException e) {
            iter.remove();
            addError(sinkgroupName, CONFIG_ERROR, ERROR);
            LOGGER.warn(
                "Could not configure sink group {} due to: {}",
                new Object[]{sinkgroupName, e.getMessage(), e}
            );
          }
        } else {
          iter.remove();
          addError(sinkgroupName, CONFIG_ERROR, ERROR);
          LOGGER.warn("Configuration error for: {}.Removed.", sinkgroupName);
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
          LOGGER.warn(
              "Agent configuration for '{}' sinkgroup '{}' sink '{}' in use by another group: " +
                  "'{}', sink not added",
              new Object[]{agentName, groupConf.getComponentName(), curSink, usedSinks.get(curSink)}
          );
          addError(groupConf.getComponentName(), PROPERTY_PART_OF_ANOTHER_GROUP, ERROR);
          sinkIt.remove();
          continue;
        } else if (!sinkSet.contains(curSink)) {
          LOGGER.warn("Agent configuration for '{}' sinkgroup '{}' sink not found: '{}', " +
                  " sink not added",
              new Object[]{agentName, groupConf.getComponentName(), curSink}
          );
          addError(curSink, INVALID_PROPERTY, ERROR);
          sinkIt.remove();
          continue;
        } else {
          usedSinks.put(curSink, groupConf.getComponentName());
        }
      }
      return groupSinks;
    }

    private String getSpaceDelimitedList(Set<String> entries) {
      if (entries.isEmpty()) {
        return null;
      }

      StringBuilder sb = new StringBuilder();

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
      sb.append(agentName).append("]").append(NEWLINE);
      sb.append("CONFIG_FILTERS: ").append(configFilterContextMap).append(NEWLINE);
      sb.append("SOURCES: ").append(sourceContextMap).append(NEWLINE);
      sb.append("CHANNELS: ").append(channelContextMap).append(NEWLINE);
      sb.append("SINKS: ").append(sinkContextMap).append(NEWLINE);

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
      // Check for configFilters
      if (CONFIG_CONFIGFILTERS.equals(key)) {
        if (configFilters == null) {
          configFilters = value;
          return true;
        } else {
          LOGGER.warn("Duplicate configfilter list specified for agent: {}", agentName);
          addError(CONFIG_CONFIGFILTERS, DUPLICATE_PROPERTY, ERROR);
          return false;
        }
      }
      // Check for sources
      if (CONFIG_SOURCES.equals(key)) {
        if (sources == null) {
          sources = value;
          return true;
        } else {
          LOGGER.warn("Duplicate source list specified for agent: {}", agentName);
          addError(CONFIG_SOURCES, DUPLICATE_PROPERTY, ERROR);
          return false;
        }
      }

      // Check for sinks
      if (CONFIG_SINKS.equals(key)) {
        if (sinks == null) {
          sinks = value;
          LOGGER.info("Added sinks: {} Agent: {}", sinks, agentName);
          return true;
        } else {
          LOGGER.warn("Duplicate sink list specfied for agent: {}", agentName);
          addError(CONFIG_SINKS, DUPLICATE_PROPERTY, ERROR);
          return false;
        }
      }

      // Check for channels
      if (CONFIG_CHANNELS.equals(key)) {
        if (channels == null) {
          channels = value;

          return true;
        } else {
          LOGGER.warn("Duplicate channel list specified for agent: {}", agentName);
          addError(CONFIG_CHANNELS, DUPLICATE_PROPERTY, ERROR);
          return false;
        }
      }

      // Check for sinkgroups
      if (CONFIG_SINKGROUPS.equals(key)) {
        if (sinkgroups == null) {
          sinkgroups = value;

          return true;
        } else {
          LOGGER.warn("Duplicate sinkgroup list specfied for agent: {}", agentName);
          addError(CONFIG_SINKGROUPS, DUPLICATE_PROPERTY, ERROR);
          return false;
        }
      }

      if (addAsSourceConfig(key, value)
          || addAsChannelValue(key, value)
          || addAsSinkConfig(key, value)
          || addAsSinkGroupConfig(key, value)
          || addAsConfigFilterConfig(key, value)
      ) {
        return true;
      }

      LOGGER.warn("Invalid property specified: {}", key);
      addError(key, INVALID_PROPERTY, ERROR);
      return false;
    }

    private boolean addAsConfigFilterConfig(String key, String value) {
      return addComponentConfig(
          key, value, CONFIG_CONFIGFILTERS_PREFIX, configFilterContextMap
      );
    }

    private boolean addAsSinkGroupConfig(String key, String value) {
      return addComponentConfig(
          key, value, CONFIG_SINKGROUPS_PREFIX, sinkGroupContextMap
      );
    }

    private boolean addAsSinkConfig(String key, String value) {
      return addComponentConfig(
          key, value, CONFIG_SINKS_PREFIX, sinkContextMap
      );
    }

    private boolean addAsChannelValue(String key, String value) {
      return addComponentConfig(
          key, value, CONFIG_CHANNELS_PREFIX, channelContextMap
      );
    }

    private boolean addAsSourceConfig(String key, String value) {
      return addComponentConfig(
          key, value, CONFIG_SOURCES_PREFIX, sourceContextMap
      );
    }

    private boolean addComponentConfig(
        String key, String value, String configPrefix, Map<String, Context> contextMap

    ) {
      ComponentNameAndConfigKey parsed = parseConfigKey(key, configPrefix);
      if (parsed != null) {
        String name = parsed.getComponentName().trim();
        LOGGER.info("Processing:{}", name);
        Context context = contextMap.get(name);

        if (context == null) {
          LOGGER.debug("Created context for {}: {}", name, parsed.getConfigKey());
          context = new Context();
          contextMap.put(name, context);
        }

        context.put(parsed.getConfigKey(), value);
        return true;
      }

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
      if (name.isEmpty() || configKey.isEmpty()) {
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
