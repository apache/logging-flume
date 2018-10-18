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
package org.apache.flume.api;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.flume.FlumeException;
import org.apache.flume.util.SSLUtil;

public abstract class SSLContextAwareAbstractRpcClient extends AbstractRpcClient {
  protected boolean enableSsl;
  protected boolean trustAllCerts;
  protected String truststore;
  protected String truststorePassword;
  protected String truststoreType;
  protected final Set<String> excludeProtocols = new LinkedHashSet<>(Arrays.asList("SSLv3"));
  protected final Set<String> includeProtocols = new LinkedHashSet<>();
  protected final Set<String> excludeCipherSuites = new LinkedHashSet<>();
  protected final Set<String> includeCipherSuites = new LinkedHashSet<>();

  protected void configureSSL(Properties properties) throws FlumeException {
    enableSsl = Boolean.parseBoolean(properties.getProperty(
      RpcClientConfigurationConstants.CONFIG_SSL));
    trustAllCerts = Boolean.parseBoolean(properties.getProperty(
      RpcClientConfigurationConstants.CONFIG_TRUST_ALL_CERTS));
    truststore = properties.getProperty(
      RpcClientConfigurationConstants.CONFIG_TRUSTSTORE, SSLUtil.getGlobalTruststorePath());
    truststorePassword = properties.getProperty(
      RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_PASSWORD,
      SSLUtil.getGlobalTruststorePassword());
    truststoreType = properties.getProperty(
      RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_TYPE,
      SSLUtil.getGlobalTruststoreType("JKS"));
    parseList(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_EXCLUDE_PROTOCOLS,
        SSLUtil.getGlobalExcludeProtocols()),
        excludeProtocols);
    parseList(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_INCLUDE_PROTOCOLS,
        SSLUtil.getGlobalIncludeProtocols()),
        includeProtocols);
    parseList(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_EXCLUDE_CIPHER_SUITES,
        SSLUtil.getGlobalExcludeCipherSuites()),
        excludeCipherSuites);
    parseList(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_INCLUDE_CIPHER_SUITES,
        SSLUtil.getGlobalIncludeCipherSuites()),
        includeCipherSuites);
  }

  private void parseList(String value, Set<String> set) {
    if (Objects.nonNull(value)) {
      set.addAll(Arrays.asList(value.split(" ")));
    }
  }
}
