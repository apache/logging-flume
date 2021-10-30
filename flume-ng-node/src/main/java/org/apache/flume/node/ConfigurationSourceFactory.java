/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.node;

import java.net.URI;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.flume.node.net.AuthorizationProvider;

/**
 * Creates ConfigurationSources.
 */
public interface ConfigurationSourceFactory {

  static ConfigurationSource getConfigurationSource(URI uri,
      AuthorizationProvider authorizationProvider, boolean verifyHost) {

    String protocol = uri.getScheme();
    final ServiceLoader<ConfigurationSourceFactory> serviceLoader =
        ServiceLoader.load(ConfigurationSourceFactory.class,
        ConfigurationSourceFactory.class.getClassLoader());
    for (final ConfigurationSourceFactory configurationSourceFactory : serviceLoader) {
      if (configurationSourceFactory.getSchemes().contains(protocol)) {
        return configurationSourceFactory.createConfigurationSource(uri, authorizationProvider,
            verifyHost);
      }
    }
    return null;
  }

  List<String> getSchemes();

  ConfigurationSource createConfigurationSource(URI uri,
      AuthorizationProvider authorizationProvider, boolean verifyHost);
}
