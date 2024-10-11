/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.node;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.conf.FlumeConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based configuration implementation provider.
 * 
 * The Agent configuration can be uploaded in ZooKeeper under a base name, which
 * defaults to /flume
 * 
 * Currently the agent configuration is stored under the agent name node in
 * ZooKeeper
 * 
 * <PRE>
 *   /flume
 *       /a1 [agent config file]
 *       /a2 [agent config file]
 *       /a3 [agent config file]
 * </PRE>
 * 
 * Configuration format is same as PropertiesFileConfigurationProvider
 * 
 * Configuration properties
 * 
 * agentName - Name of Agent for which configuration needs to be pulled
 * 
 * zkConnString - Connection string to ZooKeeper Ensemble
 * (host:port,host1:port1)
 * 
 * basePath - Base Path where agent configuration needs to be stored. Defaults
 * to /flume
 */
public abstract class AbstractZooKeeperConfigurationProvider extends
    AbstractConfigurationProvider {

  static final String DEFAULT_ZK_BASE_PATH = "/flume";

  protected final String basePath;

  protected final String zkConnString;

  private static final String DEFAULT_PROPERTIES_IMPLEMENTATION = "java.util.Properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(
      AbstractZooKeeperConfigurationProvider.class);

  protected AbstractZooKeeperConfigurationProvider(String agentName,
      String zkConnString, String basePath) {
    super(agentName);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zkConnString),
        "Invalid Zookeeper Connection String %s", zkConnString);
    this.zkConnString = zkConnString;
    if (basePath == null || basePath.isEmpty()) {
      this.basePath = DEFAULT_ZK_BASE_PATH;
    } else {
      this.basePath = basePath;
    }
  }

  protected CuratorFramework createClient() {
    return CuratorFrameworkFactory.newClient(zkConnString,
        new ExponentialBackoffRetry(1000, 1));
  }

  protected FlumeConfiguration configFromBytes(byte[] configData)
      throws IOException {
    Map<String, String> configMap = Collections.emptyMap(); 
    if (configData != null && configData.length > 0) {
      try {
        String fileContent = new String(configData, Charsets.UTF_8);
        String resolverClassName = System.getProperty("propertiesImplementation",
            DEFAULT_PROPERTIES_IMPLEMENTATION);
        Class<? extends Properties> propsclass = Class.forName(resolverClassName)
            .asSubclass(Properties.class);
        Properties properties = propsclass.newInstance();
        properties.load(new StringReader(fileContent));
        configMap = toMap(properties);
      } catch (ClassNotFoundException e) {
        LOGGER.error("Configuartion resolver class not found", e);
      } catch (InstantiationException e) {
        LOGGER.error("Instantiation exception", e);
      } catch (IllegalAccessException e) {
        LOGGER.error("Illegal access exception", e);
      }
    }
    return new FlumeConfiguration(configMap);
  }
}
