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
package org.apache.flume.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SSLUtil.class);

  private static final String SYS_PROP_KEYSTORE_PATH = "javax.net.ssl.keyStore";
  private static final String SYS_PROP_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  private static final String SYS_PROP_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  private static final String SYS_PROP_TRUSTSTORE_PATH = "javax.net.ssl.trustStore";
  private static final String SYS_PROP_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  private static final String SYS_PROP_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";

  private static final String ENV_VAR_KEYSTORE_PATH = "FLUME_SSL_KEYSTORE_PATH";
  private static final String ENV_VAR_KEYSTORE_PASSWORD = "FLUME_SSL_KEYSTORE_PASSWORD";
  private static final String ENV_VAR_KEYSTORE_TYPE = "FLUME_SSL_KEYSTORE_TYPE";
  private static final String ENV_VAR_TRUSTSTORE_PATH = "FLUME_SSL_TRUSTSTORE_PATH";
  private static final String ENV_VAR_TRUSTSTORE_PASSWORD = "FLUME_SSL_TRUSTSTORE_PASSWORD";
  private static final String ENV_VAR_TRUSTSTORE_TYPE = "FLUME_SSL_TRUSTSTORE_TYPE";

  private static final String DESCR_KEYSTORE_PATH = "keystore path";
  private static final String DESCR_KEYSTORE_PASSWORD = "keystore password";
  private static final String DESCR_KEYSTORE_TYPE = "keystore type";
  private static final String DESCR_TRUSTSTORE_PATH = "truststore path";
  private static final String DESCR_TRUSTSTORE_PASSWORD = "truststore password";
  private static final String DESCR_TRUSTSTORE_TYPE = "truststore type";

  public static void initGlobalSSLParameters() {
    initSysPropFromEnvVar(
        SYS_PROP_KEYSTORE_PATH, ENV_VAR_KEYSTORE_PATH, DESCR_KEYSTORE_PATH);
    initSysPropFromEnvVar(
        SYS_PROP_KEYSTORE_PASSWORD, ENV_VAR_KEYSTORE_PASSWORD, DESCR_KEYSTORE_PASSWORD);
    initSysPropFromEnvVar(
        SYS_PROP_KEYSTORE_TYPE, ENV_VAR_KEYSTORE_TYPE, DESCR_KEYSTORE_TYPE);
    initSysPropFromEnvVar(
        SYS_PROP_TRUSTSTORE_PATH, ENV_VAR_TRUSTSTORE_PATH, DESCR_TRUSTSTORE_PATH);
    initSysPropFromEnvVar(
        SYS_PROP_TRUSTSTORE_PASSWORD, ENV_VAR_TRUSTSTORE_PASSWORD, DESCR_TRUSTSTORE_PASSWORD);
    initSysPropFromEnvVar(
        SYS_PROP_TRUSTSTORE_TYPE, ENV_VAR_TRUSTSTORE_TYPE, DESCR_TRUSTSTORE_TYPE);
  }

  private static void initSysPropFromEnvVar(String sysPropName, String envVarName,
                                            String description) {
    if (System.getProperty(sysPropName) != null) {
      LOGGER.debug("Global SSL " + description + " has been initialized from system property.");
    } else {
      String envVarValue = System.getenv(envVarName);
      if (envVarValue != null) {
        System.setProperty(sysPropName, envVarValue);
        LOGGER.debug("Global SSL " + description +
            " has been initialized from environment variable.");
      } else {
        LOGGER.debug("No global SSL " + description + " specified.");
      }
    }
  }

  public static String getGlobalKeystorePath() {
    return System.getProperty(SYS_PROP_KEYSTORE_PATH);
  }

  public static String getGlobalKeystorePassword() {
    return System.getProperty(SYS_PROP_KEYSTORE_PASSWORD);
  }

  public static String getGlobalKeystoreType(String defaultValue) {
    String sysPropValue = System.getProperty(SYS_PROP_KEYSTORE_TYPE);
    return sysPropValue != null ? sysPropValue : defaultValue;
  }

  public static String getGlobalTruststorePath() {
    return System.getProperty(SYS_PROP_TRUSTSTORE_PATH);
  }

  public static String getGlobalTruststorePassword() {
    return System.getProperty(SYS_PROP_TRUSTSTORE_PASSWORD);
  }

  public static String getGlobalTruststoreType(String defaultValue) {
    String sysPropValue = System.getProperty(SYS_PROP_TRUSTSTORE_TYPE);
    return sysPropValue != null ? sysPropValue : defaultValue;
  }

}
