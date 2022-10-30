/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.flume.CounterGroup;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.node.net.AuthorizationProvider;
import org.apache.flume.node.net.UrlConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpConfigurationSource implements ConfigurationSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpConfigurationSource.class);
  private static final int NOT_MODIFIED = 304;
  private static final int NOT_AUTHORIZED = 401;
  private static final int NOT_FOUND = 404;
  private static final int OK = 200;
  private static final int BUF_SIZE = 1024;

  private final URI uri;
  private final CounterGroup counterGroup;
  private final AuthorizationProvider authorizationProvider;
  private final boolean verifyHost;
  private long lastModified = 0;
  private byte[] data = null;

  public HttpConfigurationSource(URI uri, AuthorizationProvider authorizationProvider,
      boolean verifyHost) {
    this.authorizationProvider = authorizationProvider;
    this.uri = uri;
    this.verifyHost = verifyHost;
    counterGroup = new CounterGroup();
    readInputStream();
  }

  @Override
  public InputStream getInputStream() {
    return new ByteArrayInputStream(data);
  }

  @Override
  public String getUri() {
    return this.uri.toString();
  }

  @Override
  public String getExtension() {
    int length = uri.getPath().indexOf(".");
    if (length <= 1) {
      return PROPERTIES;
    }
    return uri.getPath().substring(length + 1);
  }

  @Override
  public boolean isModified() {
    LOGGER.debug("Checking {} for changes", uri);

    counterGroup.incrementAndGet("uri.checks");
    try {
      LOGGER.info("Reloading configuration from:{}", uri);
      if (readInputStream()) {
        counterGroup.incrementAndGet("uri.loads");
        return true;
      }
    } catch (ConfigurationException ex) {
      LOGGER.error("Unable to access configuration due to {}: ", ex.getMessage());
    }
    return false;
  }

  private boolean readInputStream() {
    try {
      HttpURLConnection connection = UrlConnectionFactory.createConnection(uri.toURL(),
          authorizationProvider, lastModified, verifyHost);
      connection.connect();
      try {
        int code = connection.getResponseCode();
        switch (code) {
          case NOT_MODIFIED: {
            LOGGER.debug("Configuration Not Modified");
            return false;
          }
          case OK: {
            try (InputStream is = connection.getInputStream()) {
              lastModified = connection.getLastModified();
              LOGGER.debug("Content was modified for {}. lastModified: {}", uri.toString(), lastModified);
              data = IOUtils.toByteArray(is);
              return true;
            } catch (final IOException e) {
              try (InputStream es = connection.getErrorStream()) {
                LOGGER.info("Error accessing configuration at {}: {}", uri, readStream(es));
              } catch (final IOException ioe) {
                LOGGER.error("Error accessing configuration at {}: {}", uri, e.getMessage());
              }
              throw new ConfigurationException("Unable to access " + uri.toString(), e);
            }
          }
          case NOT_FOUND: {
            throw new ConfigurationException("Unable to locate " + uri.toString());
          }
          case NOT_AUTHORIZED: {
            throw new ConfigurationException("Authorization failed");
          }
          default: {
            if (code < 0) {
              LOGGER.info("Invalid response code returned");
            } else {
              LOGGER.info("Unexpected response code returned {}", code);
            }
            return false;
          }
        }
      } finally {
        connection.disconnect();
      }
    } catch (IOException e) {
      LOGGER.warn("Error accessing {}: {}", uri.toString(), e.getMessage());
      throw new ConfigurationException("Unable to access " + uri.toString(), e);
    }
  }

  private byte[] readStream(InputStream is) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUF_SIZE];
    int length;
    while ((length = is.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toByteArray();
  }

  @Override
  public String toString() {
    return "{ uri:" + uri + "}";
  }
}
