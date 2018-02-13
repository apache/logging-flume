/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.configfilter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCredentialStoreConfigFilter extends AbstractConfigFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      HadoopCredentialStoreConfigFilter.class);

  static final String PASSWORD_FILE_CONFIG_KEY
      = "credstore.java-keystore-provider.password-file";
  static final String CREDENTIAL_PROVIDER_PATH
      = "credential.provider.path";
  static final String HADOOP_SECURITY = "hadoop.security.";
  private Configuration hadoopConfiguration;

  public void initializeWithConfiguration(Map<String, String> configuration) {

    LOGGER.debug("Initializing hadoop credential store.");
    hadoopConfiguration = new Configuration();
    hadoopConfiguration.set(
        HADOOP_SECURITY + CREDENTIAL_PROVIDER_PATH,
        configuration.get(CREDENTIAL_PROVIDER_PATH)
    );

    String passwordFile = configuration.get(PASSWORD_FILE_CONFIG_KEY);
    if (passwordFile != null && !passwordFile.isEmpty()) {
      checkPasswordFile(passwordFile);
      hadoopConfiguration.set(
          HADOOP_SECURITY + PASSWORD_FILE_CONFIG_KEY, passwordFile
      );
    }
  }

  private void checkPasswordFile(String passwordFile) {
    if (Thread.currentThread().getContextClassLoader().getResource(passwordFile) == null) {
      LOGGER.error("The java keystore provider password file has to be on the classpath." +
          " The password file provided in the configuration cannot be found and will not be used"
      );
    }
  }

  @Override
  public String filter(String key) {
    char[] result = null;
    try {
      result = hadoopConfiguration.getPassword(key);
    } catch (IOException e) {
      LOGGER.error("Error while reading value for key {}: ", key, e);
    }

    return result == null ? null : new String(result);
  }


}
