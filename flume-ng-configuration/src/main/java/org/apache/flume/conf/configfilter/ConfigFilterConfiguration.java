/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.conf.configfilter;

import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

public class ConfigFilterConfiguration extends ComponentConfiguration {

  public enum ConfigFilterConfigurationType {
    OTHER(null),
    ENV("org.apache.flume.conf.configfilter.EnvironmentVariableConfigFilterConfiguration"),
    HADOOP("org.apache.flume.conf.configfilter.HadoopCredentialStoreConfigFilterConfiguration"),
    EXTERNAL("org.apache.flume.conf.configfilter.ExternalProcessConfigFilterConfiguration");

    private final String configurationName;

    ConfigFilterConfigurationType(String type) {
      configurationName = type;
    }

    public String getConfigFilterConfigurationType() {
      return configurationName;
    }

    @SuppressWarnings("unchecked")
    public ConfigFilterConfiguration getConfiguration(String name)
        throws ConfigurationException {
      if (this == OTHER) {
        return new ConfigFilterConfiguration(name);
      }
      Class<? extends ConfigFilterConfiguration> clazz;
      ConfigFilterConfiguration instance = null;
      try {
        if (configurationName != null) {
          clazz =
              (Class<? extends ConfigFilterConfiguration>) Class
                  .forName(configurationName);
          instance = clazz.getConstructor(String.class).newInstance(name);
        } else {
          return new ConfigFilterConfiguration(name);
        }
      } catch (ClassNotFoundException e) {
        // Could not find the configuration stub, do basic validation
        instance = new ConfigFilterConfiguration(name);
        // Let the caller know that this was created because of this exception.
        instance.setNotFoundConfigClass();
      } catch (Exception e) {
        throw new ConfigurationException("Couldn't create configuration", e);
      }
      return instance;
    }
  }

  protected ConfigFilterConfiguration(String componentName) {
    super(componentName);
  }

}
