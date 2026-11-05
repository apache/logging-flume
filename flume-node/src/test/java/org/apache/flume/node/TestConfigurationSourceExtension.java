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

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that the file extension of a configuration source is determined correctly.
 */
public class TestConfigurationSourceExtension {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testGetExtension() {
        Assert.assertEquals("yml", ConfigurationSource.getExtension("/etc/flume/flume-conf.yml"));
        Assert.assertEquals("yaml", ConfigurationSource.getExtension("/etc/flume.d/flume-conf.yaml"));
        Assert.assertEquals("json", ConfigurationSource.getExtension("/opt/flume-1.12.0/conf/flume.conf.json"));
        Assert.assertEquals("json", ConfigurationSource.getExtension("flume-conf.JSON"));
        Assert.assertEquals("properties", ConfigurationSource.getExtension("flume-conf.properties"));
    }

    @Test
    public void testNoExtension() {
        Assert.assertEquals("properties", ConfigurationSource.getExtension(null));
        Assert.assertEquals("properties", ConfigurationSource.getExtension(""));
        Assert.assertEquals("properties", ConfigurationSource.getExtension("/etc/flume.d/flume-conf"));
        Assert.assertEquals("properties", ConfigurationSource.getExtension("/etc/flume/.flume"));
        Assert.assertEquals("properties", ConfigurationSource.getExtension("/etc/flume/flume-conf."));
    }

    @Test
    public void testFileConfigurationSource() throws Exception {
        File dir = tempFolder.newFolder("flume.d");
        Assert.assertEquals("yml", new FileConfigurationSource(createFile(dir, "flume-conf.yml")).getExtension());
        Assert.assertEquals("json", new FileConfigurationSource(createFile(dir, "flume.conf.json")).getExtension());
        Assert.assertEquals("properties", new FileConfigurationSource(createFile(dir, "flume-conf")).getExtension());
    }

    @Test
    public void testClasspathConfigurationSource() throws Exception {
        String[] uris = {
            "classpath:///flume-conf-init.yml",
            "classpath:/flume-conf-init.yml",
            "classpath://flume-conf-init.yml",
            "classpath:flume-conf-init.yml"
        };
        for (String uri : uris) {
            Assert.assertEquals(uri, "yml", new ClasspathConfigurationSource(new URI(uri)).getExtension());
        }
    }

    private URI createFile(File dir, String name) throws Exception {
        File file = new File(dir, name);
        Files.write(file.toPath(), "a = b".getBytes(StandardCharsets.UTF_8));
        return file.toURI();
    }
}
