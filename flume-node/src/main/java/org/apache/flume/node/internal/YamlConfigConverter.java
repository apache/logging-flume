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
package org.apache.flume.node.internal;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/**
 * Converts YAML and/or JSON files into Java properties that can be consumed by the UriConfigurationProvider.
 * This uses version 3.x of SnakeYaml that is hosted at https://codeberg.org/snakeyaml/snakeyaml-engine
 * and supports YAML 1.2.
 */
public class YamlConfigConverter {

    public static Properties convertYamlToProperties(InputStream yamlInputStream) {
        Properties properties = new Properties();

        LoadSettings settings = LoadSettings.builder().build();
        Load load = new Load(settings);

        // 2. Load the YAML content into basic Java structures
        Object yamlData = load.loadFromInputStream(yamlInputStream);

        // 3. Recursively flatten the data structure
        if (yamlData instanceof Map) {
            flattenMap("", (Map<?, ?>) yamlData, properties);
        } else if (yamlData != null) {
            throw new IllegalArgumentException("Root YAML element must be a mapping object.");
        }

        return properties;
    }

    private static void flattenMap(String prefix, Map<?, ?> map, Properties properties) {
        map.forEach((key, value) -> {
            String currentKey = prefix.isEmpty() ? String.valueOf(key) : prefix + "." + key;

            if (value instanceof Map) {
                Map<?, ?> childMap = (Map<?, ?>) value;
                if (!prefix.isEmpty() && isComponentMap(childMap)) {
                    properties.setProperty(currentKey, String.join(" ", keyNames(childMap)));
                }
                flattenMap(currentKey, childMap, properties);
            } else if (value instanceof List) {
                flattenList(currentKey, (List<?>) value, properties);
            } else if (value != null) {
                properties.setProperty(currentKey, String.valueOf(value));
            }
        });
    }

    /**
     * Determines whether a Map represents a set of named components, such as the sources, channels
     * or sinks of an agent. This is the case when every key is a String and every value is a Map.
     */
    private static boolean isComponentMap(Map<?, ?> map) {
        if (map.isEmpty()) {
            return false;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof Map)) {
                return false;
            }
        }
        return true;
    }

    private static List<String> keyNames(Map<?, ?> map) {
        List<String> names = new ArrayList<>(map.size());
        map.keySet().forEach(key -> names.add((String) key));
        return names;
    }

    private static void flattenList(String prefix, List<?> list, Properties properties) {
        // Flume expects a list of simple values, such as channel names, as a space separated value.
        if (list.stream().noneMatch(value -> value instanceof Map || value instanceof List)) {
            List<String> values = new ArrayList<>(list.size());
            list.stream().filter(value -> value != null).forEach(value -> values.add(String.valueOf(value)));
            if (!values.isEmpty()) {
                properties.setProperty(prefix, String.join(" ", values));
            }
            return;
        }
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            String currentKey = prefix + "[" + i + "]";

            if (value instanceof Map) {
                flattenMap(currentKey, (Map<?, ?>) value, properties);
            } else if (value instanceof List) {
                flattenList(currentKey, (List<?>) value, properties);
            } else if (value != null) {
                properties.setProperty(currentKey, String.valueOf(value));
            }
        }
    }
}
