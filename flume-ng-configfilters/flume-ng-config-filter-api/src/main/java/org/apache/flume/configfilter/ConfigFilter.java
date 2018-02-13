/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.configfilter;

import java.util.Map;

/**
 * ConfigFilter is a tool for replacing sensitive or generated data in Flume configuration
 *
 */
public interface ConfigFilter {

  /**
   * Filter method that returns the value associated with the given key
   *
   * @param key the key to look up in the concrete implementations
   * @return the value represented by the key
   */
  String filter(String key);

  /**
   * Sets the component name. Required by the configuration management.
   *
   * @param name
   */
  void setName(String name);


  /**
   * Returns the component name. Required by the configuration management.
   *
   * @return String the component name
   */
  String getName();

  /**
   * A method to configure the component
   *
   * @param configuration The map of configuration options needed by concrete implementations.
   */
  void initializeWithConfiguration(Map<String, String> configuration);
}
