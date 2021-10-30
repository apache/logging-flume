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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;

/**
 * Resolves replaceable tokens to create a Map.
 * <p>
 * Needs org.apache.commons:commons-lang3 on classpath
 */
final class MapResolver {

  private static final String PROPS_IMPL_KEY = "propertiesImplementation";
  private static final String ENV_VAR_PROPERTY = "org.apache.flume.node.EnvVarResolverProperties";

  public static Map<String, String> resolveProperties(Properties properties) {
    Map<String, String> map = new HashMap<>();
    boolean useEnvVars = ENV_VAR_PROPERTY.equals(System.getProperty(PROPS_IMPL_KEY));
    StringLookup defaultLookup = useEnvVars ? new DefaultLookup(map) :
        StringLookupFactory.INSTANCE.mapStringLookup(map);
    StringLookup lookup = StringLookupFactory.INSTANCE.interpolatorStringLookup(defaultLookup);
    StringSubstitutor substitutor = new StringSubstitutor(lookup);
    substitutor.setEnableSubstitutionInVariables(true);
    properties.stringPropertyNames().forEach((k) -> map.put(k,
        substitutor.replace(properties.getProperty(k))));
    return map;
  }

  private static class DefaultLookup implements StringLookup {
    private final Map<String, String> properties;

    DefaultLookup(Map<String, String> properties) {
      this.properties = properties;
    }

    /**
     * Provide compatibility with EnvVarResolverProperties.
     *
     * @param key The key.
     * @return The value associated with the key or null.
     */
    @Override
    public String lookup(String key) {
      return properties.containsKey(key) ?
          properties.get(key) : System.getenv(key);
    }
  }
}
