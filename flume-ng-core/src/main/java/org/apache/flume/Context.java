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

package org.apache.flume;

import java.util.HashMap;
import java.util.Map;

public class Context {

  private Map<String, Object> parameters;

  public Context() {
    parameters = new HashMap<String, Object>();
  }

  public void put(String key, Object value) {
    parameters.put(key, value);
  }

  public <T> T get(String key, Class<? extends T> clazz) {
    if (parameters.containsKey(key)) {
      return clazz.cast(parameters.get(key));
    }

    return null;
  }

  public <T> T get(String key, Class<? extends T> clazz, T defaultValue) {
    T result = get(key, clazz);
    if (result == null) {
      result = defaultValue;
    }

    return result;
  }

  public String getString(String key) {
    return get(key, String.class);
  }

  public String getString(String key, String defaultValue) {
    return get(key, String.class, defaultValue);
  }

  @Override
  public String toString() {
    return "{ parameters:" + parameters + " }";
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public void clear() {
    parameters.clear();
  }
}
