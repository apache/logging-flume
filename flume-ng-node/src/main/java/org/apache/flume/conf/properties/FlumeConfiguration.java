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

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * FlumeConfiguration is an in memory representation of the hierarchical
 * configuration namespace required by the PropertiesFileConfgurationProvider.
 * This class is instantiated with a properties object which is parsed to
 * construct the hierarchy in memory. Once the entire set of properties have
 * been parsed and populated, a validation routine is run that identifies and
 * removes invalid components.
 * </p>
 *
 * @see org.apache.flume.conf.properties.PropertiesFileConfigurationProvider
 *
 */
public class FlumeConfiguration {

  private static final Logger logger = LoggerFactory
      .getLogger(FlumeConfiguration.class);

  private static final String NEWLINE = System.getProperty("line.separator",
      "\n");
  private static final String INDENTSTEP = "  ";

  private static final String SOURCES = "sources";
  private static final String SOURCES_PREFIX = SOURCES + ".";
  private static final String SINKS = "sinks";
  private static final String SINKS_PREFIX = SINKS + ".";
  private static final String SINKGROUPS = "sinkgroups";
  private static final String SINKGROUPS_PREFIX = SINKGROUPS + ".";
  private static final String CHANNELS = "channels";
  private static final String CHANNELS_PREFIX = CHANNELS + ".";
  private static final String RUNNER = "runner";
  private static final String RUNNER_PREFIX = RUNNER + ".";
  private static final String ATTR_TYPE = "type";
  private static final String ATTR_SINKS = "sinks";
  private static final String ATTR_CHANNEL = "channel";
  private static final String ATTR_CHANNELS = "channels";
  private static final String CLASS_CHANNEL = "channel";
  private static final String CLASS_SOURCE = "source";
  private static final String CLASS_SINK = "sink";
  private static final String CLASS_SINKGROUP = "sinkgroup";

  private final Map<String, AgentConfiguration> agentConfigMap;

  /**
   * Creates an empty Flume Configuration object.
   */
  public FlumeConfiguration(Properties properties) {
    agentConfigMap = new HashMap<String, AgentConfiguration>();

    // Construct the in-memory component hierarchy
    Enumeration<?> propertyNames = properties.propertyNames();

    while (propertyNames.hasMoreElements()) {
      String name = (String) propertyNames.nextElement();
      String value = properties.getProperty(name);

      if (!addRawProperty(name, value)) {
        logger.warn("Configuration property ignored: " + name + " = " + value);
      }
    }

    // validate and remove improperly configured components
    validateConfiguration();
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

        it.remove();
      }
    }

    logger.info("Post-validation flume configuration contains configuation "
        + " for agents: " + agentConfigMap.keySet());
  }

  private boolean addRawProperty(String name, String value) {
    // Null names and values not supported
    if (name == null || value == null) {
      return false;
    }

    // Empty values are not supported
    if (value.trim().length() == 0) {
      return false;
    }

    // Remove leading and trailing spaces
    name = name.trim();
    value = value.trim();

    int index = name.indexOf('.');

    // All configuration keys must have a prefix defined as agent name
    if (index == -1) {
      return false;
    }

    String agentName = name.substring(0, index);

    // Agent name must be specified for all properties
    if (agentName.length() == 0) {
      return false;
    }

    String configKey = name.substring(index + 1);

    // Configuration key must be specified for every property
    if (configKey.length() == 0) {
      return false;
    }

    AgentConfiguration aconf = agentConfigMap.get(agentName);

    if (aconf == null) {
      aconf = new AgentConfiguration(agentName);
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

    private AgentConfiguration(String agentName) {
      this.agentName = agentName;

      sourceConfigMap = new HashMap<String, ComponentConfiguration>();
      sinkConfigMap = new HashMap<String, ComponentConfiguration>();
      channelConfigMap = new HashMap<String, ComponentConfiguration>();
      sinkgroupConfigMap = new HashMap<String, ComponentConfiguration>();
    }

    public Collection<ComponentConfiguration> getChannels() {
      return channelConfigMap.values();
    }

    public Collection<ComponentConfiguration> getSources() {
      return sourceConfigMap.values();
    }

    public Collection<ComponentConfiguration> getSinks() {
      return sinkConfigMap.values();
    }

    public Collection<ComponentConfiguration> getSinkGroups() {
      return sinkgroupConfigMap.values();
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
      logger.debug("Starting validation of configuration for agent: "
          + agentName + ", initial-configuration: " + this);

      // Make sure that at least one channel is specified
      if (channels == null || channels.trim().length() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' does not contain any channels. Marking it as invalid.");
        return false;
      }

      Set<String> channelSet = stringToSet(channels, " \t");
      validateComponent(channelSet, channelConfigMap, CLASS_CHANNEL, ATTR_TYPE);

      if (channelSet.size() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' does not contain any valid channels. Marking it as invalid.");

        return false;
      }

      Set<String> sourceSet = validateSources(channelSet);
      Set<String> sinkSet = validateSinks(channelSet);
      Set<String> sinkgroupSet = validateGroups(sinkSet);

      // If no sources or sinks are present, then this is invalid
      if (sourceSet.size() == 0 && sinkSet.size() == 0) {
        logger.warn("Agent configuration for '" + agentName
            + "' has no sources or sinks. Will be marked invalid.");
        return false;
      }

      // Now rewrite the sources/sinks/channels

      this.sources = getSpaceDelimitedList(sourceSet);
      this.channels = getSpaceDelimitedList(channelSet);
      this.sinks = getSpaceDelimitedList(sinkSet);
      this.sinkgroups = getSpaceDelimitedList(sinkgroupSet);

      return true;
    }

    private Set<String> validateSources(Set<String> channelSet) {
      Preconditions.checkArgument(channelSet != null && channelSet.size() > 0);
      Set<String> sourceSet = stringToSet(sources, " \t");

      if (sourceSet != null && sourceSet.size() > 0) {
        // Filter out any sources that have invalid channels
        Iterator<String> srcIt = sourceConfigMap.keySet().iterator();

        while (srcIt.hasNext()) {
          String nextSource = srcIt.next();
          ComponentConfiguration sourceConfig = sourceConfigMap.get(nextSource);
          Set<String> srcChannelSet = new HashSet<String>();

          if (sourceConfig.hasAttribute(ATTR_CHANNELS)) {
            String srcChannels = sourceConfig.getAttribute(ATTR_CHANNELS);
            StringTokenizer srcChTok = new StringTokenizer(srcChannels, " \t");

            while (srcChTok.hasMoreTokens()) {
              String nextSrcCh = srcChTok.nextToken();

              if (channelSet.contains(nextSrcCh)) {
                srcChannelSet.add(nextSrcCh);
              } else {
                logger.warn("Agent configuration for '" + agentName
                    + "' source '" + sourceConfig.getComponentName()
                    + "' contains invalid channel: '" + nextSrcCh
                    + "'. Will be removed.");
              }
            }
          }

          if (srcChannelSet.size() == 0) {
            logger.warn("Agent configuration for '" + agentName + "' source '"
                + sourceConfig.getComponentName()
                + "' has no valid channels. Removing.");

            srcIt.remove();
            continue;
          }

          // Override the source configuration to reset channels
          StringBuilder validSrcChannelBuilder = new StringBuilder("");

          for (String validSrcCh : srcChannelSet) {
            validSrcChannelBuilder.append(" ").append(validSrcCh);
          }

          sourceConfig.setAttribute(ATTR_CHANNELS, validSrcChannelBuilder
              .toString().trim());
        }
      }

      validateComponent(sourceSet, sourceConfigMap, CLASS_SOURCE, ATTR_TYPE,
          ATTR_CHANNELS);
      return sourceSet;
    }

    private Set<String> validateSinks(Set<String> channelSet) {
      Preconditions.checkArgument(channelSet != null && channelSet.size() > 0);
      Set<String> sinkSet = stringToSet(sinks, " \t");

      if (sinkSet != null && sinkSet.size() > 0) {
        // Filter out any sinks that have invalid channel
        Iterator<String> sinkIt = sinkConfigMap.keySet().iterator();

        while (sinkIt.hasNext()) {
          String nextSink = sinkIt.next();
          ComponentConfiguration sinkConfig = sinkConfigMap.get(nextSink);

          if (sinkConfig.hasAttribute(ATTR_CHANNEL)) {
            String sinkCh = sinkConfig.getAttribute(ATTR_CHANNEL);

            if (!channelSet.contains(sinkCh)) {
              logger.warn("Agent configuration for '" + agentName + "' sink '"
                  + sinkConfig.getComponentName() + "' has invalid channel '"
                  + sinkCh + "' specified. Removing.");
              sinkIt.remove();
              continue;
            }
          } else {
            logger.warn("Agent configuration for '" + agentName + "' sink '"
                + sinkConfig.getComponentName()
                + "' has no channels. Removing.");

            sinkIt.remove();
            continue;
          }
        }
      }

      validateComponent(sinkSet, sinkConfigMap, CLASS_SINK, ATTR_TYPE,
          ATTR_CHANNEL);
      return sinkSet;
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
      Set<String> sinkgroupSet = stringToSet(sinkgroups, " \t");
      if(sinkgroupSet != null && sinkgroupSet.size() > 0) {
        Iterator<String> groupIt = sinkgroupConfigMap.keySet().iterator();
        Map<String, String> usedSinks = new HashMap<String, String>();
        while (groupIt.hasNext()) {
          String nextGroup = groupIt.next();
          ComponentConfiguration groupConf = sinkgroupConfigMap.get(nextGroup);

          if ( ! groupConf.hasAttribute(ATTR_SINKS)) {
            logger.warn("Agent configuration for '" + agentName
                + "' sinkGroup '" + groupConf.getComponentName()
                + "' has no configured sinks. Removing.");
            groupIt.remove();
            continue;
          }
          Set<String> groupSinks = validGroupSinks(sinkSet, usedSinks,
              groupConf);
          if (groupSinks == null || groupSinks.isEmpty()) {
            logger.warn("Agent configuration for '" + agentName
                + "' sinkGroup '" + groupConf.getComponentName()
                + "' has no valid sinks. Removing.");
            groupIt.remove();
          } else {
            groupConf.setAttribute(ATTR_SINKS,
                this.getSpaceDelimitedList(groupSinks));
          }

        }
      }
      validateComponent(sinkgroupSet, sinkgroupConfigMap, CLASS_SINKGROUP,
          ATTR_SINKS);
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
        Map<String, String> usedSinks, ComponentConfiguration groupConf) {
      Set<String> groupSinks = stringToSet(groupConf.getAttribute(ATTR_SINKS),
          " \t");
      if(groupSinks == null) return null;
      Iterator<String> sinkIt = groupSinks.iterator();
      while (sinkIt.hasNext()) {
        String curSink = sinkIt.next();
        if(usedSinks.containsKey(curSink)) {
          logger.warn("Agent configuration for '" + agentName + "' sinkgroup '"
              + groupConf.getComponentName() + "' sink '" + curSink
              + "' in use by " + "another group: '" + usedSinks.get(curSink)
              + "', sink not added");
          sinkIt.remove();
          continue;
        } else if (!sinkSet.contains(curSink)) {
          logger.warn("Agent configuration for '" + agentName + "' sinkgroup '"
              + groupConf.getComponentName() + "' sink not found: '" + curSink
              + "',  sink not added");
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

    private Set<String> stringToSet(String target, String delim) {
      Set<String> out = new HashSet<String>();
      if(target == null || target.trim().length() == 0) {
        return out;
      }
      StringTokenizer t = new StringTokenizer(target, delim);
      while(t.hasMoreTokens()) {
        out.add(t.nextToken());
      }
      return out;
    }

    /**
     * <p>
     * Utility method to iterate over the component configuration to validate
     * them based on the criteria as follows:
     * <ol>
     * <li>Each component in the configuredMap must be present in the given
     * activeSet</li>
     * <li>Each component in activeSet must be configured in the configuredMap</li>
     * <li>Each component must have requiredAttributes set correctly.</li>
     * </ol>
     *
     * @param activeSet
     *          the active set of components
     * @param configuredMap
     *          the components picked from configuration
     * @param componentClass
     *          the component class - source, sink, etc
     * @param requiredAttributes
     *          the required attributes for the component
     */
    private void validateComponent(Set<String> activeSet,
        Map<String, ComponentConfiguration> configuredMap,
        String componentClass, String... requiredAttributes) {

      Iterator<String> it = configuredMap.keySet().iterator();

      while (it.hasNext()) {
        String componentName = it.next();

        if (!activeSet.contains(componentName)) {
          logger.warn("Agent configuration for '" + agentName + "': "
              + componentClass + " '" + componentName
              + "' not in active list. Removing.");

          it.remove();
          continue;
        }

        // Every component must have a required attribute
        ComponentConfiguration config = configuredMap.get(componentName);
        boolean missingRequiredAttributes = false;

        for (String attrName : requiredAttributes) {
          if (!config.hasAttribute(attrName)) {
            logger.warn("Agent configuration for '" + agentName + "': "
                + componentClass + " '" + componentName + "' does not have '"
                + attrName + "' specified.");
            missingRequiredAttributes = true;
          }
        }

        if (missingRequiredAttributes) {
          logger.warn("Agent configuration for '" + agentName + "': "
              + componentClass + " '" + componentName
              + "' has some required attributes missing. Removing.");

          it.remove();
          continue;
        }
      }

      // Remove the active components that are not configured
      Iterator<String> activeIt = activeSet.iterator();

      while (activeIt.hasNext()) {
        String componentName = activeIt.next();

        if (!configuredMap.containsKey(componentName)) {
          logger.warn("Agent configuration for '" + agentName + "': "
              + componentClass + " '" + componentName
              + "' is not configured. Removing.");

          activeIt.remove();
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("AgentConfiguration[");
      sb.append(agentName).append("]").append(NEWLINE).append("SOURCES: ");
      sb.append(sourceConfigMap).append(NEWLINE).append("CHANNELS: ");
      sb.append(channelConfigMap).append(NEWLINE).append("SINKS: ");
      sb.append(sinkConfigMap);

      return sb.toString();
    }

    private boolean addProperty(String key, String value) {
      // Check for sources
      if (key.equals(SOURCES)) {
        if (sources == null) {
          sources = value;
          return true;
        } else {
          logger
          .warn("Duplicate source list specified for agent: " + agentName);
          return false;
        }
      }

      // Check for sinks
      if (key.equals(SINKS)) {
        if (sinks == null) {
          sinks = value;
          return true;
        } else {
          logger.warn("Duplicate sink list specfied for agent: " + agentName);
          return false;
        }
      }

      // Check for channels
      if (key.equals(CHANNELS)) {
        if (channels == null) {
          channels = value;

          return true;
        } else {
          logger.warn("Duplicate channel list specified for agent: "
              + agentName);

          return false;
        }
      }

      // Check for sinkgroups
      if (key.equals(SINKGROUPS))  {
        if (sinkgroups == null) {
          sinkgroups = value;

          return true;
        } else {
          logger.warn("Duplicate channel list specfied for agent: "
              + agentName);
          return false;
        }
      }

      ComponentNameAndConfigKey cnck = parseConfigKey(key, SOURCES_PREFIX);

      if (cnck != null) {
        // it is a source
        String name = cnck.getComponentName();
        ComponentConfiguration srcConf = sourceConfigMap.get(name);

        if (srcConf == null) {
          srcConf = new ComponentConfiguration(name, true);
          sourceConfigMap.put(name, srcConf);
        }

        return srcConf.addProperty(cnck.getConfigKey(), value);
      }

      cnck = parseConfigKey(key, CHANNELS_PREFIX);

      if (cnck != null) {
        // it is a channel
        String name = cnck.getComponentName();
        ComponentConfiguration channelConf = channelConfigMap.get(name);

        if (channelConf == null) {
          channelConf = new ComponentConfiguration(name, false);
          channelConfigMap.put(name, channelConf);
        }

        return channelConf.addProperty(cnck.getConfigKey(), value);
      }

      cnck = parseConfigKey(key, SINKS_PREFIX);

      if (cnck != null) {
        // it is a sink
        String name = cnck.getComponentName();
        ComponentConfiguration sinkConf = sinkConfigMap.get(name);

        if (sinkConf == null) {
          sinkConf = new ComponentConfiguration(name, true);
          sinkConfigMap.put(name, sinkConf);
        }

        return sinkConf.addProperty(cnck.getConfigKey(), value);
      }

      cnck = parseConfigKey(key, SINKGROUPS_PREFIX);

      if (cnck != null) {
        String name = cnck.getComponentName();
        ComponentConfiguration groupConf = sinkgroupConfigMap.get(name);
        if(groupConf == null) {
          groupConf = new ComponentConfiguration(name, true);
          sinkgroupConfigMap.put(name, groupConf);
        }

        return groupConf.addProperty(cnck.getConfigKey(), value);
      }

      logger.warn("Invalid property specified: " + key);
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

  public static class ComponentConfiguration {

    private final String componentName;
    private final boolean hasRunner;
    private final ComponentConfiguration runnerConfig;

    private final Map<String, String> configuration;

    private ComponentConfiguration(String componentName, boolean hasRunner) {
      this.componentName = componentName;
      this.hasRunner = hasRunner;

      if (hasRunner) {
        runnerConfig = new ComponentConfiguration(RUNNER, false);
      } else {
        runnerConfig = null;
      }

      this.configuration = new HashMap<String, String>();
    }

    @Override
    public String toString() {
      return toString(0);
    }

    public String getComponentName() {
      return componentName;
    }

    private boolean hasAttribute(String attributeName) {
      return configuration.containsKey(attributeName);
    }

    private String getAttribute(String attriubteName) {
      return configuration.get(attriubteName);
    }

    private void setAttribute(String attributeName, String value) {
      configuration.put(attributeName, value);
    }

    private String toString(int indentCount) {
      StringBuilder indentSb = new StringBuilder("");

      for (int i = 0; i < indentCount; i++) {
        indentSb.append(INDENTSTEP);
      }

      String indent = indentSb.toString();
      StringBuilder sb = new StringBuilder(indent);

      sb.append("ComponentConfiguration[").append(componentName).append("]");
      sb.append(NEWLINE).append(indent).append(INDENTSTEP).append("CONFIG: ");
      sb.append(configuration);
      sb.append(NEWLINE).append(indent).append(INDENTSTEP);

      if (hasRunner) {
        sb.append("RUNNER: ").append(runnerConfig.toString(indentCount + 1));
      }

      sb.append(NEWLINE);

      return sb.toString();
    }

    private boolean addProperty(String key, String value) {
      // see if the key belongs to the runner
      if (hasRunner && key.startsWith(RUNNER_PREFIX)) {
        String subKey = key.substring(RUNNER_PREFIX.length());
        if (subKey.length() == 0) {
          logger.warn("Invalid key specified: " + key);
          return false;
        }
        return runnerConfig.addProperty(subKey, value);
      }

      // do not allow properties of the name "runner"
      if (hasRunner && key.equals(RUNNER)) {
        logger.warn("Cannot have property named: " + key + " for component: "
            + componentName);
        return false;
      }

      if (!configuration.containsKey(key)) {
        configuration.put(key, value);
        return true;
      }

      logger.warn("Duplicate property '" + key + "' specified for "
          + componentName);
      return false;
    }

    public Map<String, String> getConfiguration() {
      return configuration;
    }

    public Map<String, String> getSubconfiguration(String namespace) {
      Map<String, String> result = new HashMap<String, String>();
      String prefix = namespace + ".";

      for (String property : configuration.keySet()) {
        if (property.startsWith(prefix)) {
          result.put(property.substring(prefix.length()),
              configuration.get(property));
        }
      }

      return result;
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
