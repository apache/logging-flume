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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.FlumeConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestUriConfigurationProviderBackup {

    private static final String AGENT = "host1";

    @TempDir
    File backupDir;

    private static ConfigurationSource classpath(String name) throws Exception {
        return new ClasspathConfigurationSource(new URI("classpath:///" + name));
    }

    private static List<ConfigurationSource> brokenSources() throws Exception {
        return List.of(classpath("flume-conf-init.json"), classpath("flume-conf-init-missing.yml"));
    }

    @Test
    public void usesBackupWhenPrimaryFails() throws Exception {
        UriConfigurationProvider good = new UriConfigurationProvider(
                AGENT, List.of(classpath("flume-conf-init.json")), backupDir.getPath(), null, 0);
        assertNotNull(good.getFlumeConfiguration());
        assertTrue(new File(backupDir, "." + AGENT + ".properties").exists());

        UriConfigurationProvider broken =
                new UriConfigurationProvider(AGENT, brokenSources(), backupDir.getPath(), null, 0);
        FlumeConfiguration config = broken.getFlumeConfiguration();
        assertNotNull(config);
        assertNotNull(config.getConfigurationFor(AGENT));
    }

    @Test
    public void throwsWhenPrimaryFailsWithoutBackupDirectory() throws Exception {
        UriConfigurationProvider broken = new UriConfigurationProvider(AGENT, brokenSources(), null, null, 0);
        assertThrows(ConfigurationException.class, broken::getFlumeConfiguration);
    }

    @Test
    public void throwsWhenPrimaryFailsAndNoBackupFileExists() throws Exception {
        UriConfigurationProvider broken =
                new UriConfigurationProvider(AGENT, brokenSources(), backupDir.getPath(), null, 0);
        assertThrows(ConfigurationException.class, broken::getFlumeConfiguration);
    }

    @Test
    public void reloadFailureDoesNotUseBackupOnceLoaded() throws Exception {
        ToggleSource toggle = new ToggleSource("host1.channels.channel1.type = memory\n");
        UriConfigurationProvider provider =
                new UriConfigurationProvider(AGENT, List.of(toggle), backupDir.getPath(), null, 0);
        assertNotNull(provider.getFlumeConfiguration());
        assertTrue(new File(backupDir, "." + AGENT + ".properties").exists());

        toggle.fail = true;
        assertThrows(ConfigurationException.class, provider::getFlumeConfiguration);
    }

    /** A properties source that can be switched to fail on demand. */
    private static final class ToggleSource implements ConfigurationSource {
        private final String content;
        private volatile boolean fail = false;

        ToggleSource(String content) {
            this.content = content;
        }

        @Override
        public InputStream getInputStream() {
            return fail ? null : new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String getUri() {
            return "toggle:test.properties";
        }

        @Override
        public String getExtension() {
            return PROPERTIES;
        }
    }
}
