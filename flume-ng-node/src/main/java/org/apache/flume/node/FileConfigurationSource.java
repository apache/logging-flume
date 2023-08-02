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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.flume.CounterGroup;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConfigurationSource implements ConfigurationSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileConfigurationSource.class);

  private final Path path;
  private final URI uri;
  private final CounterGroup counterGroup;
  private byte[] data;
  private long lastChange;

  public FileConfigurationSource(URI uri) {
    this.uri = uri;
    this.path = Paths.get(uri);
    counterGroup = new CounterGroup();
    try {
      this.lastChange = path.toFile().lastModified();
      data = Files.readAllBytes(this.path);
    } catch (IOException ioe) {
      LOGGER.error("Unable to read {}: {}", path.toString(), ioe.getMessage());
      throw new ConfigurationException("Unable to read file " + path.toString(), ioe);
    }
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
    LOGGER.debug("Checking file:{} for changes", path.toString());

    counterGroup.incrementAndGet("file.checks");

    long lastModified = path.toFile().lastModified();

    if (lastModified > lastChange) {
      LOGGER.info("Reloading configuration file:{}", path.toString());

      counterGroup.incrementAndGet("file.loads");

      lastChange = lastModified;

      try {
        data = Files.readAllBytes(path);
        return true;
      } catch (Exception e) {
        LOGGER.error("Failed to load configuration data. Exception follows.", e);
      } catch (NoClassDefFoundError e) {
        LOGGER.error("Failed to start agent because dependencies were not found in classpath."
            + "Error follows.", e);
      } catch (Throwable t) {
        // caught because the caller does not handle or log Throwables
        LOGGER.error("Unhandled error", t);
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "{ file:" + path.toString() + "}";
  }
}
