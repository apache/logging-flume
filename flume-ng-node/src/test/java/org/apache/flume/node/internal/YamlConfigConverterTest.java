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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileInputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class YamlConfigConverterTest {

    @Test
    public void testYaml() {
        try {
            FileInputStream is = new FileInputStream("target/test-classes/flume-conf-init.yml");
            Properties props = YamlConfigConverter.convertYamlToProperties(is);
            assertNotNull(props);
            is = new FileInputStream("target/test-classes/flume-conf-init.properties");
            Properties properties = new Properties();
            properties.load(is);
            System.out.println(props.size() + " properties");
            for (String name : props.stringPropertyNames()) {
                System.out.println(name + ": " + props.get(name));
            }
            assertEquals(properties, props);
            assertNull(props.getProperty("host1"));
        } catch (Exception ex) {
            fail("Unable to convert YAML to properties: " + ex.getMessage());
        }
    }

    @Test
    public void testJson() {
        try {
            FileInputStream is = new FileInputStream("target/test-classes/flume-conf-init.json");
            Properties props = YamlConfigConverter.convertYamlToProperties(is);
            assertNotNull(props);
            is = new FileInputStream("target/test-classes/flume-conf-init.properties");
            Properties properties = new Properties();
            properties.load(is);
            assertEquals(properties, props);
        } catch (Exception ex) {
            fail("Unable to convert JSON to properties: " + ex.getMessage());
        }
    }
}
