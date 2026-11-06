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

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.node.net.AuthorizationProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OptionalConfigurationSource implements ConfigurationSource {
    private static final Logger LOGGER = LogManager.getLogger(OptionalConfigurationSource.class);
    private static final String OPTIONAL = "optional:";
    private final ConfigurationSource source;
    private final URI realUri;

    public OptionalConfigurationSource(URI uri, AuthorizationProvider authorizationProvider, boolean verifyHost) {
        String path = uri.toString().substring(OPTIONAL.length());
        try {
            realUri = new URI(path);
        } catch (URISyntaxException use) {
            throw new ConfigurationException("Invalid URI");
        }
        source = ConfigurationSourceFactory.getConfigurationSource(realUri, authorizationProvider, verifyHost);
        if (source == null) {
            LOGGER.warn("Unknown source for {}", path);
        }
    }

    @Override
    public InputStream getInputStream() {
        InputStream is = null;
        if (source == null) {
            is = InputStream.nullInputStream();
        } else {
            try {
                is = source.getInputStream();
                if (is == null) {
                    is = InputStream.nullInputStream();
                }
            } catch (Exception ex) {
                is = InputStream.nullInputStream();
            }
        }
        return is;
    }

    @Override
    public String getUri() {
        return source == null ? realUri.toString() : source.getUri();
    }

    @Override
    public String getExtension() {
        return source == null ? ConfigurationSource.getExtension(realUri.getPath()) : source.getExtension();
    }

    @Override
    public String toString() {
        if (source == null) {
            return "optional: null";
        }
        return "optional " + source.toString();
    }
}
