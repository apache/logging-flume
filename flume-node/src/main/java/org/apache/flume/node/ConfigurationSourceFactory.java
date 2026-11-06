/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.flume.node.net.AuthorizationProvider;

/**
 * Creates ConfigurationSources.
 */
public interface ConfigurationSourceFactory {
    String OPTIONAL = "optional:";

    static ConfigurationSource getConfigurationSource(
            URI uri, AuthorizationProvider authorizationProvider, boolean verifyHost) throws URISyntaxException {
        String uriStr = uri.toString();
        boolean isOptional = false;
        if (uriStr.startsWith(OPTIONAL)) {
            isOptional = true;
            uri = new URI(uriStr.substring(OPTIONAL.length()));
        }
        String protocol = uri.getScheme();
        final ServiceLoader<ConfigurationSourceFactory> serviceLoader =
                ServiceLoader.load(ConfigurationSourceFactory.class, ConfigurationSourceFactory.class.getClassLoader());
        for (final ConfigurationSourceFactory configurationSourceFactory : serviceLoader) {
            if (configurationSourceFactory.getSchemes().contains(protocol)) {
                return configurationSourceFactory.createConfigurationSource(
                        uri, authorizationProvider, verifyHost, isOptional);
            }
        }
        return null;
    }

    List<String> getSchemes();

    ConfigurationSource createConfigurationSource(
            URI uri, AuthorizationProvider authorizationProvider, boolean verifyHost, boolean isOptional);
}
