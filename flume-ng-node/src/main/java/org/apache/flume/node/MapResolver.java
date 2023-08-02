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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.DefaultStringLookup;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves replaceable tokens to create a Map.
 * <p>
 * Needs org.apache.commons:commons-lang3 on classpath
 */
final class MapResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapResolver.class);
  private static final String DEFAULT_LOOKUPS = "lookups.properties";
  private static final String CUSTOM_LOOKUPS_KEY = "lookups";
  private static final String PROPS_IMPL_KEY = "propertiesImplementation";
  private static final String ENV_VAR_PROPERTY = "org.apache.flume.node.EnvVarResolverProperties";
  private static final String LOOKUP  = "org.apache.commons.text.lookup.DefaultStringLookup.";
  private static final LookupEntry[] LOOKUP_ENTRIES = {
    new LookupEntry("sys", DefaultStringLookup.SYSTEM_PROPERTIES.getStringLookup()),
    new LookupEntry("env", DefaultStringLookup.ENVIRONMENT.getStringLookup())  ,
    new LookupEntry("java", DefaultStringLookup.JAVA.getStringLookup()),
    new LookupEntry("date", DefaultStringLookup.DATE.getStringLookup())
  };

  public static Map<String, String> resolveProperties(Properties properties) {
    Map<String, String> map = new HashMap<>();
    boolean useEnvVars = ENV_VAR_PROPERTY.equals(System.getProperty(PROPS_IMPL_KEY));
    StringLookup defaultLookup = useEnvVars ? new DefaultLookup(map) :
        StringLookupFactory.INSTANCE.mapStringLookup(map);
    StringLookup lookup = StringLookupFactory.INSTANCE.interpolatorStringLookup(createLookupMap(),
        defaultLookup, false);
    StringSubstitutor substitutor = new StringSubstitutor(lookup);
    substitutor.setEnableSubstitutionInVariables(true);
    properties.stringPropertyNames().forEach((k) -> map.put(k,
        substitutor.replace(properties.getProperty(k))));
    return map;
  }

  private static Map<String, StringLookup> createLookupMap() {
    Map<String, StringLookup> map = new HashMap<>();
    Properties properties = loadProperties();
    if (properties == null) {
      Arrays.stream(LOOKUP_ENTRIES).forEach((e) -> {
        map.put(e.key, e.lookup);
      });
    } else {
      properties.forEach((k, v) -> {
        String key = Objects.toString(k);
        String value = Objects.toString(v);
        if (value.startsWith(LOOKUP)) {
          String lookupEnum = value.substring(LOOKUP.length());
          try {
            StringLookup stringLookup = DefaultStringLookup.valueOf(lookupEnum).getStringLookup();
            map.put(key.toLowerCase(Locale.ROOT), stringLookup);
          } catch (IllegalArgumentException ex) {
            LOGGER.warn("{} is not a DefaultStringLookup enum value, ignoring", key);
          }
        } else {
          try {
            Class<?> clazz = Class.forName(Objects.toString(v));
            if (StringLookup.class.isAssignableFrom(clazz)) {
              StringLookup stringLookup = (StringLookup) clazz.newInstance();
              map.put(k.toString().toLowerCase(Locale.ROOT), stringLookup);
            } else {
              LOGGER.warn("{} is not a StringLookup, ignoring", v);
            }
          } catch (Exception ex) {
            LOGGER.warn("Unable to load {} due to {}, ignoring", v, ex.getMessage());
          }
        }
      });
    }
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

  private static class LookupEntry {
    private final String key;
    private final StringLookup lookup;

    public LookupEntry(String key, StringLookup lookup) {
      this.key = key;
      this.lookup = lookup;
    }
  }

  private static Properties loadProperties() {
    final Properties properties = new Properties();
    String fileName = System.getProperty(CUSTOM_LOOKUPS_KEY);
    if (fileName != null) {
      try (InputStream inputStream = new FileInputStream(fileName)) {
        properties.load(inputStream);
      } catch (final IOException e) {
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(fileName)) {
          properties.load(inputStream);
        } catch (final IOException ex) {
          LOGGER.warn("Unable to load {} due to {}", fileName, ex.getMessage());
        }
      }
    }
    if (properties.size() == 0) {
      try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(DEFAULT_LOOKUPS)) {
        if (inputStream != null) {
          properties.load(inputStream);
        } else {
          return null;
        }
      } catch (final IOException e) {
        return null;
      }
    }
    return properties;
  }
}
