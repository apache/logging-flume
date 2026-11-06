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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.flume.conf.ConfigurationException;
import org.junit.jupiter.api.Test;

public class TestCreateConfigMap {

    @Test
    public void testCreateConfigMap() throws Exception {
        URI uri1 = new URI("classpath:///flume-conf-init.json");
        ConfigurationSource source1 = new ClasspathConfigurationSource(uri1);
        URI uri2 = new URI("classpath:///flume-conf-init-override.yml");
        ConfigurationSource source2 = new ClasspathConfigurationSource(uri2);
        URI uri3 = new URI("optional:classpath:///flume-conf-init-missing.yml");
        ConfigurationSource source3 = new OptionalConfigurationSource(uri3, null, false);
        List<ConfigurationSource> sources = List.of(source1, source2, source3);
        Map<String, String> configMap = UriConfigurationProvider.createConfigMap(sources, true);
        assertNotNull(configMap);
        assertTrue(configMap.containsKey("host1.sources.source1.command"));
        String command = configMap.get("host1.sources.source1.command");
        assertEquals("ls -1", command);
    }

    @Test
    public void testOptionalPresentCreateConfigMap() throws Exception {
        URI uri1 = new URI("classpath:///flume-conf-init.json");
        ConfigurationSource source1 = new ClasspathConfigurationSource(uri1);
        URI uri2 = new URI("classpath:///flume-conf-init-override.yml");
        ConfigurationSource source2 = new ClasspathConfigurationSource(uri2);
        URI uri3 = new URI("optional:classpath:///flume-conf-init-extra.yml");
        ConfigurationSource source3 = new OptionalConfigurationSource(uri3, null, false);
        List<ConfigurationSource> sources = List.of(source1, source2, source3);
        Map<String, String> configMap = UriConfigurationProvider.createConfigMap(sources, true);
        assertNotNull(configMap);
        assertTrue(configMap.containsKey("host1.channels.channel1.capacity"));
        String capacity = configMap.get("host1.channels.channel1.capacity");
        assertEquals("30000", capacity);
    }

    @Test
    public void failCreateConfigMap() throws Exception {
        URI uri1 = new URI("classpath:///flume-conf-init.json");
        ConfigurationSource source1 = new ClasspathConfigurationSource(uri1);
        URI uri2 = new URI("classpath:///flume-conf-init-override.yml");
        ConfigurationSource source2 = new ClasspathConfigurationSource(uri2);
        URI uri3 = new URI("classpath:///flume-conf-init-missing.yml");
        ConfigurationSource source3 = new ClasspathConfigurationSource(uri3);
        List<ConfigurationSource> sources = List.of(source1, source2, source3);
        assertThrows(ConfigurationException.class, () -> UriConfigurationProvider.createConfigMap(sources, true));
    }
}
