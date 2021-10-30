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

import java.io.InputStream;

/**
 * Interface for retrieving configuration data.
 */
public interface ConfigurationSource {

  static final String PROPERTIES = "properties";
  static final String JSON = "json";
  static final String YAML = "yaml";
  static final String XML = "xml";

  /**
   * Returns the InputStream if it hasn't already been processed.
   * @return The InputStream or null.
   */
  InputStream getInputStream();

  /**
   * Returns the URI string.
   * @return The string URI.
   */
  String getUri();

  /**
   * Determine if the configuration data source has been modified since it was last checked.
   * @return true if the data was modified.
   */
  default boolean isModified() {
    return false;
  }

  /**
   * Return the "file" extension for the specified uri.
   * @return The file extension.
   */
  String getExtension();
}
